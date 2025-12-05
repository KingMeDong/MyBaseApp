using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MyBase.Data;
using MyBase.Models.Finance;

namespace MyBase.Services.MarketData {

    /// <summary>
    /// BackfillWorker (Daily Chunking):
    /// - Iteriert Tag für Tag durch den Zeitraum (period=1d, bar=1min)
    /// - Robustere Retries pro Tag
    /// - Realtime-Zeilen werden niemals überschrieben
    /// </summary>
    public sealed class BackfillWorker : BackgroundService {
        private readonly IServiceProvider _sp;
        private readonly ILogger<BackfillWorker> _log;
        private readonly Channel<BackfillRequest> _queue;
        private readonly BackfillStatusStore _status;
        private readonly IHttpClientFactory _http;

        public BackfillWorker(
            IServiceProvider sp,
            ILogger<BackfillWorker> log,
            Channel<BackfillRequest> queue,
            BackfillStatusStore statusStore,
            IHttpClientFactory http) {
            _sp = sp;
            _log = log;
            _queue = queue;
            _status = statusStore;
            _http = http;
        }

        protected override async Task ExecuteAsync(CancellationToken ct) {
            SafeLog("BackfillWorker: gestartet (warte auf Aufträge).");

            while (!ct.IsCancellationRequested) {
                BackfillRequest req;
                try {
                    req = await _queue.Reader.ReadAsync(ct);
                } catch (OperationCanceledException) { break; } catch (ChannelClosedException) { break; } catch (Exception ex) {
                    SafeLog($"BackfillWorker: Queue-Fehler – {ex.Message}");
                    _log.LogError(ex, "Queue.ReadAsync failed");
                    await Task.Delay(250, ct);
                    continue;
                }

                var st = _status.Create(req);
                st.SetState(BackfillJobState.Running);

                try {
                    using var scope = _sp.CreateScope();
                    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                    // --- Instrument ---
                    Instrument? inst;
                    try {
                        inst = await db.Instruments.AsNoTracking()
                                   .FirstOrDefaultAsync(i => i.Id == req.InstrumentId, ct);
                        if (inst is null) {
                            MarkFail(st, "Instrument nicht gefunden.");
                            continue;
                        }
                        st.InstrumentSymbol = inst.Symbol;
                    } catch (Exception ex) {
                        MarkFail(st, $"Instrument-Lookup fehlgeschlagen: {ex.Message}");
                        continue;
                    }

                    SafeLog($"BackfillWorker: Job angenommen (JobId={st.JobId}, {inst.Symbol}, {req.StartUtc:u} → {req.EndUtc:u}).");

                    var client = _http.CreateClient("Cpapi");
                    
                    // Tageweise iterieren
                    var currentDay = req.StartUtc.Date;
                    var endDay = req.EndUtc.Date;
                    
                    int totalFetched = 0;
                    int totalInserted = 0;
                    int totalUpdated = 0;
                    int totalSkipped = 0;

                    while (currentDay <= endDay && !ct.IsCancellationRequested) {
                        SafeLog($"Backfill: Bearbeite Tag {currentDay:yyyy-MM-dd}...");

                        // 1. Fetch Day
                        List<Candle> dayCandles;
                        try {
                            dayCandles = await FetchDailyChunkAsync(client, inst.IbConId, currentDay, req.RthOnly, ct);
                        } catch (Exception ex) {
                            SafeLog($"Backfill: Fehler beim Abrufen von {currentDay:yyyy-MM-dd}: {ex.Message}");
                            currentDay = currentDay.AddDays(1);
                            continue;
                        }

                        if (dayCandles.Count == 0) {
                            SafeLog($"Backfill: Keine Daten für {currentDay:yyyy-MM-dd}.");
                        } else {
                            totalFetched += dayCandles.Count;
                            // 2. Save Day
                            var (ins, upd, skip) = await SaveBarsAsync(db, inst.Id, dayCandles, req.Source ?? "backfill", ct);
                            totalInserted += ins;
                            totalUpdated += upd;
                            totalSkipped += skip;
                        }

                        st.IncSegmentDone(); // Fortschritt grob tracken
                        
                        // Nächster Tag
                        currentDay = currentDay.AddDays(1);
                        
                        // Pacing
                        await Task.Delay(300, ct);
                    }

                    _status.TrySetFinished(st.JobId, BackfillJobState.Done);
                    SafeLog($"BackfillWorker: Job abgeschlossen (JobId={st.JobId}).");
                    SafeLog($"BackfillWorker: SUMMARY -> Fetched={totalFetched}, Inserted={totalInserted}, Updated={totalUpdated}, SkippedRealtime={totalSkipped}");

                } catch (OperationCanceledException) {
                    // shutdown
                } catch (Exception ex) {
                    SafeLog($"BackfillWorker: UNERWARTETER FEHLER – {ex.Message}");
                    _log.LogError(ex, "BackfillWorker: unexpected error");
                    _status.TrySetFinished(st.JobId, BackfillJobState.Failed, ex.Message);
                }
            }

            SafeLog("BackfillWorker: gestoppt.");
        }

        // ------------------------------ Fetch Logic (Daily) ------------------------------
        
        private async Task<List<Candle>> FetchDailyChunkAsync(HttpClient client, long conId, DateTime dayUtc, bool rthOnly, CancellationToken ct) {
            // Parameter analog Python Script
            // period=1d, bar=1min, startTime=YYYYMMDD-HH:mm:ss
            // barType=midpoint (bleiben wir dabei, wie besprochen)
            
            var startTimeStr = dayUtc.ToString("yyyyMMdd-HH:mm:ss", CultureInfo.InvariantCulture);
            var outsideRth = (!rthOnly).ToString().ToLowerInvariant();
            var url = $"/v1/api/iserver/marketdata/history?conid={conId}&period=1d&bar=1min&outsideRth={outsideRth}&barType=midpoint&startTime={startTimeStr}";

            const int maxRetries = 5;
            const int retryDelayMs = 5000; // 5s

            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    using var r = await client.GetAsync(url, ct);
                    var code = (int)r.StatusCode;
                    var body = await r.Content.ReadAsStringAsync(ct);

                    if (r.IsSuccessStatusCode) {
                        var list = ParseHistory(body);
                        SafeLog($"Backfill: {dayUtc:yyyy-MM-dd} -> {list.Count} Bars (Versuch {attempt}).");
                        return list;
                    }

                    // Fehlerbehandlung
                    SafeLog($"Backfill: {dayUtc:yyyy-MM-dd} Fehler {code} (Versuch {attempt}/{maxRetries}). Body: {TrimForLog(body)}");
                    
                    if (attempt < maxRetries) {
                        await Task.Delay(retryDelayMs, ct);
                    }
                } catch (Exception ex) {
                    SafeLog($"Backfill: Exception {dayUtc:yyyy-MM-dd} (Versuch {attempt}/{maxRetries}): {ex.Message}");
                    if (attempt < maxRetries) {
                        await Task.Delay(retryDelayMs, ct);
                    }
                }
            }

            SafeLog($"Backfill: {dayUtc:yyyy-MM-dd} -> Überspringe Tag nach {maxRetries} Versuchen.");
            return new List<Candle>();
        }

        // ------------------------------ DB Save Logic ------------------------------

        private async Task<(int Inserted, int Updated, int Skipped)> SaveBarsAsync(AppDbContext db, int instrumentId, List<Candle> candles, string source, CancellationToken ct) {
            if (candles.Count == 0) return (0, 0, 0);

            try {
                // Sortieren
                candles = candles.OrderBy(c => c.TsUtc).ToList();
                
                var fromUtc = candles.First().TsUtc;
                var toUtc = candles.Last().TsUtc;

                // Existierende laden
                var existingRows = await db.Bars1m
                    .Where(b => b.InstrumentId == instrumentId && b.TsUtc >= fromUtc && b.TsUtc <= toUtc)
                    .Select(b => new { b.TsUtc, b.IngestSource })
                    .ToListAsync(ct);

                var existingMap = existingRows.ToDictionary(x => x.TsUtc, x => x.IngestSource);

                var toInsert = new List<Bar1m>(candles.Count);
                var toUpdate = new List<Bar1m>();
                int skippedRealtime = 0;

                foreach (var c in candles) {
                    var tsNy = IctTime.ToNy(c.TsUtc);
                    var sess = IctTime.SessionOf(tsNy);
                    var kz = IctTime.KillzoneOf(tsNy);

                    var row = new Bar1m {
                        InstrumentId = instrumentId,
                        TsUtc = c.TsUtc,
                        TsNy = tsNy,
                        Open = (decimal)c.O,
                        High = (decimal)c.H,
                        Low = (decimal)c.L,
                        Close = (decimal)c.C,
                        Volume = 0, // midpoint
                        Session = sess,
                        Killzone = kz,
                        IngestSource = source
                    };

                    if (existingMap.TryGetValue(c.TsUtc, out var src)) {
                        if (string.Equals(src, "realtime", StringComparison.OrdinalIgnoreCase)) {
                            skippedRealtime++;
                            continue; 
                        }
                        toUpdate.Add(row);
                    } else {
                        toInsert.Add(row);
                    }
                }

                if (toInsert.Count > 0)
                    await db.Bars1m.AddRangeAsync(toInsert, ct);

                if (toUpdate.Count > 0) {
                    foreach (var e in toUpdate) {
                        db.Bars1m.Attach(e);
                        db.Entry(e).Property(x => x.TsNy).IsModified = true;
                        db.Entry(e).Property(x => x.Open).IsModified = true;
                        db.Entry(e).Property(x => x.High).IsModified = true;
                        db.Entry(e).Property(x => x.Low).IsModified = true;
                        db.Entry(e).Property(x => x.Close).IsModified = true;
                        // Volume bleibt 0
                        db.Entry(e).Property(x => x.Session).IsModified = true;
                        db.Entry(e).Property(x => x.Killzone).IsModified = true;
                        // Source nicht überschreiben
                    }
                }

                await db.SaveChangesAsync(ct);
                db.ChangeTracker.Clear();
                
                return (toInsert.Count, toUpdate.Count, skippedRealtime);

            } catch (Exception ex) {
                SafeLog($"Backfill: DB Save Error: {ex.Message}");
                _log.LogError(ex, "DB Save Error");
                return (0, 0, 0);
            }
        }

        // ------------------------------ Helpers ------------------------------

        private static List<Candle> ParseHistory(string json) {
            try {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                var arr = root;
                if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("data", out var d))
                    arr = d;

                if (arr.ValueKind != JsonValueKind.Array) return new List<Candle>();

                var list = new List<Candle>(arr.GetArrayLength());
                foreach (var el in arr.EnumerateArray())
                    if (TryParseCandle(el, out var c)) list.Add(c);

                return list;
            } catch {
                return new List<Candle>();
            }
        }

        private static bool TryParseCandle(JsonElement el, out Candle c) {
            c = default;
            if (!TryParseTs(el, "t", out var tsUtc)) return false;
            // IBKR liefert manchmal Strings, manchmal Numbers
            if (!TryGetDouble(el, "o", out var o)) return false;
            if (!TryGetDouble(el, "h", out var h)) return false;
            if (!TryGetDouble(el, "l", out var l)) return false;
            if (!TryGetDouble(el, "c", out var close)) return false;
            
            // Volumen ignorieren wir bei midpoint, aber falls wir trades nutzen würden:
            // TryGetDouble(el, "v", out var v);

            c = new Candle {
                TsUtc = tsUtc,
                O = o,
                H = h,
                L = l,
                C = close
            };
            return true;
        }

        private static bool TryParseTs(JsonElement el, string name, out DateTime tsUtc) {
            tsUtc = default;
            if (!el.TryGetProperty(name, out var p)) return false;

            if (p.ValueKind == JsonValueKind.String) {
                var s = p.GetString()!;
                // IBKR Formate variieren
                if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out tsUtc)) return true;
                if (DateTime.TryParseExact(s, "yyyyMMdd-HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var a)) {
                    tsUtc = DateTime.SpecifyKind(a, DateTimeKind.Utc);
                    return true;
                }
            } else if (p.ValueKind == JsonValueKind.Number && p.TryGetInt64(out var ms)) {
                tsUtc = DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
                return true;
            }
            return false;
        }

        private static bool TryGetDouble(JsonElement el, string name, out double v) {
            v = 0;
            if (!el.TryGetProperty(name, out var p)) return false;
            if (p.ValueKind == JsonValueKind.Number && p.TryGetDouble(out v)) return true;
            if (p.ValueKind == JsonValueKind.String && double.TryParse(p.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out v)) return true;
            return false;
        }

        private static string TrimForLog(string s)
            => string.IsNullOrWhiteSpace(s) ? "" : (s.Length <= 200 ? s : s[..200] + " …");

        private static void SafeLog(string line) {
            try { SessionLogBuffer.Append(line); } catch { /* ignore */ }
        }

        private static void MarkFail(BackfillStatus st, string msg) {
            st.Error = msg;
            st.SetState(BackfillJobState.Failed);
            st.FinishedUtc = DateTime.UtcNow;
            SafeLog($"BackfillWorker: JOB FEHLER – {msg}");
        }

        private readonly struct Candle {
            public DateTime TsUtc { get; init; }
            public double O { get; init; }
            public double H { get; init; }
            public double L { get; init; }
            public double C { get; init; }
        }
    }
}
