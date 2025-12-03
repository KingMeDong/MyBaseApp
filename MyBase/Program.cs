using System.Net;
using System.Threading.Channels;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting; // IHostApplicationLifetime
using MyBase.Data;
using MyBase.Models;
using MyBase.Models.Finance;
using MyBase.Services;
using MyBase.Services.MarketData;

var builder = WebApplication.CreateBuilder(args);

// FileHelper initialisieren
FileHelper.Initialize(builder.Configuration);

// 🔹 SessionLogBuffer (Datei-Logging) initialisieren
SessionLogBuffer.Configure(builder.Configuration);

// Kestrel-Limits
builder.WebHost.ConfigureKestrel(k => {
    k.Limits.MaxRequestBodySize = null;
});

// Konfiguration
builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true);

// Services
builder.Services.AddRazorPages();
builder.Services.AddSession();

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.Configure<SmtpSettings>(builder.Configuration.GetSection("SmtpSettings"));
builder.Services.AddHostedService<ReminderService>();

// Für Download-/Thumbnail-APIs
builder.Services.AddControllers();

// Externe Clients
builder.Services.AddHttpClient<MyBase.Clients.IoBrokerClient>();

// CPAPI Options binden
builder.Services.Configure<CpapiOptions>(builder.Configuration.GetSection("CPAPI"));

// 🔹 Gemeinsamer Cookie-Container (wichtig für Session!)
var cpapiCookies = new CookieContainer();
builder.Services.AddSingleton(cpapiCookies);

// HttpClient für CPAPI (Gateway)
builder.Services.AddHttpClient("Cpapi", (sp, c) => {
    var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<CpapiOptions>>().CurrentValue;
    c.BaseAddress = new Uri(opt.BaseUrl);
    c.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
    c.DefaultRequestHeaders.TryAddWithoutValidation("Accept", "application/json, text/plain, */*");
    c.DefaultRequestHeaders.TryAddWithoutValidation("X-Requested-With", "XMLHttpRequest");
})
.ConfigurePrimaryHttpMessageHandler(sp => {
    var cookies = sp.GetRequiredService<CookieContainer>();
    return new HttpClientHandler {
        CookieContainer = cookies,
        UseCookies = true,
        ServerCertificateCustomValidationCallback = (msg, cert, chain, errors) => true
    };
});

// 🔹 Background-Services
builder.Services.AddSingleton<SessionManager>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SessionManager>());
builder.Services.AddHostedService<MarketDataService>();

// 🔹 Backfill-Infrastruktur
builder.Services.AddSingleton(Channel.CreateUnbounded<BackfillRequest>(
    new UnboundedChannelOptions { SingleReader = true, SingleWriter = false }));
builder.Services.AddSingleton<BackfillStatusStore>();
builder.Services.AddHostedService<BackfillWorker>();

var app = builder.Build();

// Host-Lebenszyklus + sauberes Beenden loggen
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();

lifetime.ApplicationStarted.Register(() => {
    SessionLogBuffer.Info("HOST", "STARTED", "ApplicationStarted");
});

lifetime.ApplicationStopping.Register(() => {
    // erst loggen, dann Writer stoppen
    SessionLogBuffer.Warn("HOST", "STOPPING", "ApplicationStopping");
    SessionLogBuffer.Stop();
});

// Optional: globale Crash-Hooks (oben in Program.cs, vor app.Run)
AppDomain.CurrentDomain.UnhandledException += (s, e) => {
    var ex = e.ExceptionObject as Exception;
    SessionLogBuffer.Error("HOST", "UNHANDLED", "AppDomain exception",
        ("isTerminating", e.IsTerminating), ("err", ex?.Message ?? e.ExceptionObject.ToString()));
};

TaskScheduler.UnobservedTaskException += (s, e) => {
    e.SetObserved();
    SessionLogBuffer.Error("HOST", "UNOBSERVED_TASK", "TaskScheduler exception",
        ("err", e.Exception?.Message));
};

AppDomain.CurrentDomain.ProcessExit += (s, e) => {
    SessionLogBuffer.Warn("HOST", "PROCESS_EXIT", "ProcessExit");
    SessionLogBuffer.Stop();
};


// Fehlerseite/HSTS
if (!app.Environment.IsDevelopment()) {
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();

// Statische Dateien
app.UseStaticFiles();

// Statische Auslieferung der ORIGINAL-Bilder
app.UseStaticFiles(new StaticFileOptions {
    FileProvider = new PhysicalFileProvider(FileHelper.ImagesDirectory),
    RequestPath = "/media/images"
});

app.UseRouting();

app.MapControllers();
app.UseSession();
app.UseAuthorization();

// Root -> Login
app.MapGet("/", context => {
    context.Response.Redirect("/Account/Login");
    return Task.CompletedTask;
});

// Media Streaming
app.MapGet("/Media/Stream", async (HttpContext context) => {
    var pathParam = context.Request.Query["path"];
    var filePath = System.Net.WebUtility.UrlDecode(pathParam);

    if (!System.IO.File.Exists(filePath)) {
        context.Response.StatusCode = 404;
        return;
    }

    var fileInfo = new System.IO.FileInfo(filePath);
    long totalLength = fileInfo.Length;
    long start = 0;
    long end = totalLength - 1;

    string contentType = "application/octet-stream";
    var extension = Path.GetExtension(filePath).ToLowerInvariant();
    switch (extension) {
        case ".mp4": contentType = "video/mp4"; break;
        case ".webm": contentType = "video/webm"; break;
        case ".mkv": contentType = "video/x-matroska"; break;
        case ".avi": contentType = "video/x-msvideo"; break;
    }

    context.Response.Headers.Add("Accept-Ranges", "bytes");
    context.Response.ContentType = contentType;

    if (context.Request.Headers.ContainsKey("Range")) {
        var rangeHeader = context.Request.Headers["Range"].ToString();
        var range = rangeHeader.Replace("bytes=", "").Split('-');
        start = long.Parse(range[0]);
        if (range.Length > 1 && !string.IsNullOrEmpty(range[1])) {
            end = long.Parse(range[1]);
        }

        context.Response.StatusCode = 206;
        context.Response.Headers.Add("Content-Range", $"bytes {start}-{end}/{totalLength}");
    }

    long contentLength = end - start + 1;
    context.Response.ContentLength = contentLength;

    using var stream = System.IO.File.OpenRead(filePath);
    stream.Seek(start, SeekOrigin.Begin);

    byte[] buffer = new byte[64 * 1024];
    long remaining = contentLength;

    while (remaining > 0) {
        int toRead = (int)Math.Min(buffer.Length, remaining);
        int read = await stream.ReadAsync(buffer, 0, toRead);
        if (read == 0) break;

        await context.Response.Body.WriteAsync(buffer, 0, read);
        remaining -= read;
    }
});

// --- GATEWAY ---
app.MapPost("/api/gateway/start", async (AppDbContext db) => {
    var e = await db.AppSettings.FindAsync("GatewayDesiredState");
    if (e is null)
        db.AppSettings.Add(new AppSetting { Key = "GatewayDesiredState", Value = "Running" });
    else
        e.Value = "Running";
    await db.SaveChangesAsync();
    return Results.Ok(new { ok = true, desired = "Running" });
});

app.MapPost("/api/gateway/stop", async (AppDbContext db) => {
    var e = await db.AppSettings.FindAsync("GatewayDesiredState");
    if (e is null)
        db.AppSettings.Add(new AppSetting { Key = "GatewayDesiredState", Value = "Stopped" });
    else
        e.Value = "Stopped";
    await db.SaveChangesAsync();
    return Results.Ok(new { ok = true, desired = "Stopped" });
});

app.MapGet("/api/gateway/status", async (AppDbContext db, SessionManager session) => {
    var gw = (await db.AppSettings.FindAsync("GatewayDesiredState"))?.Value ?? "Running";
    return Results.Ok(new { desired = gw, session = session.State.ToString() });
});

// --- MARKETDATA ---
app.MapPost("/api/marketdata/start", async (AppDbContext db) => {
    var e = await db.AppSettings.FindAsync("MarketDataDesiredState");
    if (e is null) db.AppSettings.Add(new AppSetting { Key = "MarketDataDesiredState", Value = "Running" });
    else e.Value = "Running";
    await db.SaveChangesAsync();

    SessionLogBuffer.Append("CMD: MarketDataDesiredState -> Running (POST)");
    return Results.Ok(new { ok = true, desired = "Running" });
});

app.MapPost("/api/marketdata/stop", async (AppDbContext db) => {
    var e = await db.AppSettings.FindAsync("MarketDataDesiredState");
    if (e is null) db.AppSettings.Add(new AppSetting { Key = "MarketDataDesiredState", Value = "Stopped" });
    else e.Value = "Stopped";
    await db.SaveChangesAsync();

    SessionLogBuffer.Append("CMD: MarketDataDesiredState -> Stopped (POST)");
    return Results.Ok(new { ok = true, desired = "Stopped" });
});

// ✅ Computed Feed Status (Running/Idle/Waiting/Stopped)
app.MapGet("/api/marketdata/status", async (AppDbContext db, SessionManager session) => {
    // Feed-Desired (Realtime)
    var desired = (await db.AppSettings.FindAsync("MarketDataDesiredState"))?.Value ?? "Stopped";

    // Gateway-Desired (für Anzeige im UI)
    var gatewayDesired = (await db.AppSettings.FindAsync("GatewayDesiredState"))?.Value ?? "Running";

    var instId = await db.Instruments.AsNoTracking()
        .Where(i => i.IsActive)
        .OrderBy(i => i.Id)
        .Select(i => i.Id)
        .FirstOrDefaultAsync();

    var fs = instId == 0 ? null : await db.FeedStates.FindAsync(instId);

    DateTime utcNow = DateTime.UtcNow;
    int? rtAge = null, hbAge = null;

    if (fs?.LastRealtimeTsUtc is DateTime rtu) rtAge = (int)Math.Round((utcNow - rtu).TotalSeconds);
    if (fs?.LastHeartbeatUtc is DateTime hbu) hbAge = (int)Math.Round((utcNow - hbu).TotalSeconds);

    string feedComputed;
    var desiredRunning = string.Equals(desired, "Running", StringComparison.OrdinalIgnoreCase);
    var sessionConnected = session.State == SessionState.Connected;

    if (!desiredRunning)
        feedComputed = "Stopped";
    else if (!sessionConnected)
        feedComputed = "Waiting (session)";
    else if (rtAge.HasValue && rtAge.Value <= 120)
        feedComputed = "Running";
    else if (hbAge.HasValue && hbAge.Value <= 45)
        feedComputed = "Idle";
    else
        feedComputed = "Stopped";

    // Rückgabe: neue UND alte Feldnamen (für volle Kompatibilität)
    return Results.Ok(new {
        // Gateway
        gatewayDesired,

        // Realtime-Feed (Wunschzustand)
        desired,                // (alter Name bleibt erhalten)
        feedDesired = desired,  // (sprechender Alias)

        // Session + berechneter Feed-Status
        session = session.State.ToString(),
        feed = feedComputed,

        // Zeitstempel (neu)
        heartbeat = fs?.LastHeartbeatUtc,
        realtime = fs?.LastRealtimeTsUtc,
        error = fs?.LastError,

        // Zeitstempel (alt – Backwards-Compatibility)
        lastHeartbeatUtc = fs?.LastHeartbeatUtc,
        lastRealtimeBarUtc = fs?.LastRealtimeTsUtc,
        lastError = fs?.LastError,

        // optionale Alterswerte in Sekunden
        heartbeatAgeSec = hbAge,
        realtimeAgeSec = rtAge
    });
});

// Rolling Log fürs Terminal
app.MapGet("/api/marketdata/log", () => Results.Text(SessionLogBuffer.ReadAll(), "text/plain"));

// --- BACKFILL (ZEITRAUMFÄHIG) ---
app.MapPost("/api/backfill/start", async (IServiceProvider sp, Channel<BackfillRequest> queue, BackfillApiContracts.BackfillStartDto? body) => {
    using var scope = sp.CreateScope();
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

    // Instrument bestimmen (Standard: erstes aktives)
    var instQuery = db.Instruments.AsNoTracking().Where(i => i.IsActive);
    if (body?.InstrumentId is int iid) instQuery = db.Instruments.AsNoTracking().Where(i => i.Id == iid);

    var inst = await instQuery.OrderBy(i => i.Id).FirstOrDefaultAsync();
    if (inst == null) {
        SessionLogBuffer.Append("CMD: Backfill abgebrochen – kein (aktives) Instrument gefunden.");
        return Results.BadRequest(new { error = "No instrument" });
    }

    var nowUtc = DateTime.UtcNow;
    var endDefault = BackfillApiContracts.FloorToMinuteUtc(nowUtc).AddMinutes(-5); // vermeidet Clash mit Realtime
    var months = body?.Months ?? 6;
    var startDefault = nowUtc.AddMonths(-months);

    var start = body?.StartUtc ?? startDefault;
    var end = body?.EndUtc ?? endDefault;
    var rth = body?.RthOnly ?? true;

    if (end <= start) {
        SessionLogBuffer.Append("CMD: Backfill abgebrochen – ungültiger Zeitraum (End <= Start).");
        return Results.BadRequest(new { error = "Invalid range" });
    }

    var req = new BackfillRequest {
        InstrumentId = inst.Id,
        StartUtc = DateTime.SpecifyKind(start, DateTimeKind.Utc),
        EndUtc = DateTime.SpecifyKind(end, DateTimeKind.Utc),
        RthOnly = rth,
        Source = "backfill"
    };

    if (!queue.Writer.TryWrite(req)) {
        SessionLogBuffer.Append("CMD: Backfill konnte nicht gequeued werden (Queue voll?).");
        return Results.StatusCode(503);
    }

    SessionLogBuffer.Append($"CMD: Backfill queued (JobId={req.JobId}, {inst.Symbol}, {req.StartUtc:u} → {req.EndUtc:u}, RTH={(rth ? "true" : "false")}).");
    return Results.Accepted($"/api/backfill/status/{req.JobId}", new { jobId = req.JobId });
});

// --- BACKFILL (SHORTCUT 1 WOCHE) ---
app.MapPost("/api/backfill/start/1w", async (IServiceProvider sp, Channel<BackfillRequest> queue, BackfillApiContracts.BackfillStartDto? body) => {
    using var scope = sp.CreateScope();
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

    var instQuery = db.Instruments.AsNoTracking().Where(i => i.IsActive);
    if (body?.InstrumentId is int iid) instQuery = db.Instruments.AsNoTracking().Where(i => i.Id == iid);

    var inst = await instQuery.OrderBy(i => i.Id).FirstOrDefaultAsync();
    if (inst == null) {
        SessionLogBuffer.Append("CMD: Backfill(1w) abgebrochen – kein (aktives) Instrument gefunden.");
        return Results.BadRequest(new { error = "No instrument" });
    }

    var nowUtc = DateTime.UtcNow;
    var endUtc = BackfillApiContracts.FloorToMinuteUtc(nowUtc).AddMinutes(-5);
    var startUtc = endUtc.AddDays(-7);

    var rth = body?.RthOnly ?? true;

    if (endUtc <= startUtc) {
        SessionLogBuffer.Append("CMD: Backfill(1w) abgebrochen – ungültiger Zeitraum (End <= Start).");
        return Results.BadRequest(new { error = "Invalid range" });
    }

    var req = new BackfillRequest {
        InstrumentId = inst.Id,
        StartUtc = DateTime.SpecifyKind(startUtc, DateTimeKind.Utc),
        EndUtc = DateTime.SpecifyKind(endUtc, DateTimeKind.Utc),
        RthOnly = rth,
        Source = "backfill"
    };

    if (!queue.Writer.TryWrite(req)) {
        SessionLogBuffer.Append("CMD: Backfill(1w) konnte nicht gequeued werden (Queue voll?).");
        return Results.StatusCode(503);
    }

    SessionLogBuffer.Append($"CMD: Backfill(1w) queued (JobId={req.JobId}, {inst.Symbol}, {req.StartUtc:u} → {req.EndUtc:u}, RTH={(rth ? "true" : "false")}).");
    return Results.Accepted($"/api/backfill/status/{req.JobId}", new { jobId = req.JobId });
});

app.MapGet("/api/backfill/status/{id:guid}", (BackfillStatusStore store, Guid id) => {
    var st = store.Get(id);
    if (st is null) return Results.NotFound();

    return Results.Ok(new {
        st.JobId,
        st.State,
        st.SegmentsPlanned,
        st.SegmentsDone,
        st.Inserted,
        st.Upserts,
        st.Error,
        st.InstrumentId,
        st.InstrumentSymbol,
        st.StartUtc,
        st.EndUtc,
        st.RthOnly,
        st.Source,
        st.CreatedUtc,
        st.FinishedUtc
    });
});

// --- ANALYSIS TEST RUNNER ---
app.MapGet("/api/analysis/test", async (AppDbContext db) => {
    // 1. Daten laden (letzte 1000 Bars vom ersten aktiven Instrument)
    var inst = await db.Instruments.AsNoTracking().Where(i => i.IsActive).FirstOrDefaultAsync();
    if (inst == null) return Results.BadRequest("No active instrument");

    var bars = await db.Bars1m.AsNoTracking()
        .Where(b => b.InstrumentId == inst.Id)
        .OrderByDescending(b => b.TsUtc)
        .Take(1000)
        .ToListAsync();
    
    // Sortieren für die Analyse (alt -> neu)
    bars.Reverse();

    // 2. Detectors instanziieren
    var fvgDetector = new MyBase.Services.Trading.Analysis.FvgDetector(minGapTicks: 1);
    var structDetector = new MyBase.Services.Trading.Analysis.StructureDetector(leftBars: 3, rightBars: 3);

    // 3. Analyse laufen lassen
    // Wir iterieren durch die Historie, um zu simulieren, wie es live wäre
    // (Ein echter Backtest würde das effizienter machen, aber für den Test ok)
    var signals = new List<MyBase.Services.Trading.Analysis.Signal>();
    
    // Fenster über die Bars schieben
    // Fenster über die Bars schieben
    // Wir brauchen min. 7 Bars für Structure (3+1+3)
    for (int i = 7; i <= bars.Count; i++) {
        var window = bars.Skip(i - 7).Take(7).ToList(); // Fenstergröße anpassen wenn nötig, aber Detect nimmt eh die ganze Liste
        
        // FVG braucht nur 3, aber wir können ihm auch 7 geben (er nimmt die letzten 3)
        // ACHTUNG: Detect erwartet eine Liste und prüft oft am Ende.
        // Um effizient zu sein, übergeben wir hier einfach das Fenster.
        // Besser wäre: Detect(bars, currentIndex) -> aber unser Interface ist Detect(List<Bar>)
        
        // Workaround für Test: Wir geben immer das aktuelle Fenster rein.
        // FVG schaut auf die letzten 3.
        // Structure schaut auf die Mitte (i-4).
        
        signals.AddRange(fvgDetector.Detect(window));
        signals.AddRange(structDetector.Detect(window));
    }

    return Results.Ok(new {
        barsAnalysed = bars.Count,
        signalsFound = signals.Count,
        signals = signals.OrderByDescending(s => s.TsUtc) // Neueste zuerst
    });
});

app.MapRazorPages();

// 🔸 Globale Exception-Hooks (Prozessweite Fehler sichtbar machen)
AppDomain.CurrentDomain.UnhandledException += (s, e) => {
    var ex = e.ExceptionObject as Exception;
    SessionLogBuffer.Error("HOST", "UNHANDLED",
        "AppDomain exception",
        ("isTerminating", e.IsTerminating),
        ("err", ex?.Message ?? e.ExceptionObject.ToString()));
};

TaskScheduler.UnobservedTaskException += (s, e) => {
    // Verhindert, dass der Prozess unerwartet terminiert (je nach Host-Policy)
    e.SetObserved();
    SessionLogBuffer.Error("HOST", "UNOBSERVED_TASK",
        "TaskScheduler exception",
        ("err", e.Exception?.Message));
};

// (Optional) Wird bei "harten" Beendigungen gefeuert
AppDomain.CurrentDomain.ProcessExit += (s, e) => {
    SessionLogBuffer.Warn("HOST", "PROCESS_EXIT", "ProcessExit");
    SessionLogBuffer.Stop();
};


app.Run();


// ========== ALLE Typen/Helper NACH den Top-Level-Statements! ==========
namespace MyBase.Services.MarketData {
    /// <summary>DTO für /api/backfill/start (Zeitraum/Optionen)</summary>
    public static class BackfillApiContracts {
        public record BackfillStartDto(
            int? InstrumentId,
            DateTime? StartUtc,
            DateTime? EndUtc,
            int? Months,
            bool? RthOnly
        );

        public static DateTime FloorToMinuteUtc(DateTime dtUtc) =>
            new DateTime(dtUtc.Year, dtUtc.Month, dtUtc.Day, dtUtc.Hour, dtUtc.Minute, 0, DateTimeKind.Utc);
    }
}
