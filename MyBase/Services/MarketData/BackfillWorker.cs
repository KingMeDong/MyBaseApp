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
    /// BackfillWorker (stabil, ohne Volumen):
    /// - HMDS-Call: period=5m, bar=1min, barType=midpoint (keine Volumina)
    /// - Robustere Logs und Guards
    /// - Realtime-Zeilen werden niemals überschrieben
    /// - EF-Updates sind selektiv (kein versehentliches Überschreiben von IngestSource)
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

        // ------------------------------ Lifecycle ------------------------------
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
                        _log.LogError(ex, "Instrument lookup failed");
                        continue;
                    }

                    SafeLog($"BackfillWorker: Job angenommen (JobId={st.JobId}, {inst.Symbol}, {req.StartUtc:u} → {req.EndUtc:u}, RTH={(req.RthOnly ? "true" : "false")}).");

                    // --- HMDS Call: midpoint (stabil, ohne Volumen) ---
                    // NEU: Wir nutzen den offiziellen /iserver/ Endpoint statt /hmds/
                    const string period = "5m";
                    const string bar = "1min";
                    const string barType = "midpoint"; // stabil; liefert kein 'v'
                    var outsideRth = (!req.RthOnly).ToString().ToLowerInvariant();

                    var client = _http.CreateClient("Cpapi");
                    // ALT: /v1/api/hmds/history
                    // NEU: /v1/api/iserver/marketdata/history
                    var url = $"/v1/api/iserver/marketdata/history?conid={inst.IbConId}&period={period}&bar={bar}&outsideRth={outsideRth}&barType={barType}";
                    
                    SafeLog($"Backfill: Requesting {url}");

                    List<Candle> candles;
                    try {
                        candles = await FetchHmdsByPeriodAsync(client, url, ct);
                    } catch (Exception ex) {
                        MarkFail(st, $"HMDS-Request fehlgeschlagen: {ex.Message}");
                        _log.LogError(ex, "HMDS request failed");
                        continue;
                    }

                    // Trim auf gewünschten Zeitraum (auch wenn HMDS relativ zu 'jetzt' liefert)
                    var trimmed = candles
                        .Where(c => c.TsUtc >= req.StartUtc && c.TsUtc <= req.EndUtc)
                        .GroupBy(c => c.TsUtc)
                        .Select(g => g.First())
                        .OrderBy(c => c.TsUtc)
                        .ToList();

                    if (trimmed.Count == 0) {
                        SafeLog("BackfillWorker: Hinweis – 0 Bars im Zielzeitraum. (Server evtl. leer/cached; Job wird ohne Fehler abgeschlossen.)");
                        st.IncSegmentDone();
                        _status.TrySetFinished(st.JobId, BackfillJobState.Done);
                        continue;
                    }

                    // --- Upsert ---
                    try {
                        var fromUtc = trimmed.First().TsUtc;
                        var toUtc = trimmed.Last().TsUtc;

                        // existierende Rows (inkl. Quelle) im Bereich
                        var existingRows = await db.Bars1m
                            .Where(b => b.InstrumentId == inst.Id && b.TsUtc >= fromUtc && b.TsUtc <= toUtc)
                            .Select(b => new { b.TsUtc, b.IngestSource })
                            .ToListAsync(ct);

                        var existingMap = existingRows.ToDictionary(x => x.TsUtc, x => x.IngestSource);

                        var toInsert = new List<Bar1m>(trimmed.Count);
                        var toUpdate = new List<Bar1m>();
                        int skippedRealtime = 0;

                        foreach (var c in trimmed) {
                            var tsNy = IctTime.ToNy(c.TsUtc);
                            var sess = IctTime.SessionOf(tsNy);
                            var kz = IctTime.KillzoneOf(tsNy);

                            var row = new Bar1m {
                                InstrumentId = inst.Id,
                                TsUtc = c.TsUtc,
                                TsNy = tsNy,
                                Open = (decimal)c.O,
                                High = (decimal)c.H,
                                Low = (decimal)c.L,
                                Close = (decimal)c.C,
                                Volume = 0, // midpoint -> kein Volumen
                                Session = sess,
                                Killzone = kz,
                                IngestSource = req.Source ?? "backfill"
                            };

                            if (existingMap.TryGetValue(c.TsUtc, out var src)) {
                                if (string.Equals(src, "realtime", StringComparison.OrdinalIgnoreCase)) {
                                    skippedRealtime++;
                                    continue; // Realtime NIE überschreiben
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
                                db.Entry(e).Property(x => x.Volume).IsModified = true; // bleibt 0 (midpoint)
                                db.Entry(e).Property(x => x.Session).IsModified = true;
                                db.Entry(e).Property(x => x.Killzone).IsModified = true;

                                // Quelle niemals überschreiben
                                db.Entry(e).Property(x => x.IngestSource).IsModified = false;
                            }
                        }

                        await db.SaveChangesAsync(ct);
                        db.ChangeTracker.Clear();

                        SafeLog($"BackfillWorker: gespeichert (inserted={toInsert.Count}, updated={toUpdate.Count}, skippedRealtime={skippedRealtime}).");
                    } catch (Exception ex) {
                        MarkFail(st, $"DB-Speichern fehlgeschlagen: {ex.Message}");
                        _log.LogError(ex, "DB save failed");
                        continue;
                    }

                    st.IncSegmentDone();
                    _status.TrySetFinished(st.JobId, BackfillJobState.Done);
                    SafeLog($"BackfillWorker: Job abgeschlossen (JobId={st.JobId}).");
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

        // ------------------------------ HMDS (period=…) ------------------------------
        private static async Task<List<Candle>> FetchHmdsByPeriodAsync(HttpClient client, string url, CancellationToken ct) {
            // kleine, robuste Retry-Logik
            for (int attempt = 1; attempt <= 3; attempt++) {
                try {
                    using var r = await client.GetAsync(url, ct);
                    var body = await r.Content.ReadAsStringAsync(ct);
                    var code = (int)r.StatusCode;

                    if (r.IsSuccessStatusCode) {
                        var list = ParseHistory(body);
                        SafeLog($"Backfill: OK ({code}) – {list.Count} Bars empfangen.");
                        return list;
                    }

                    // 5xx -> Retry, 429 -> kurzer Backoff
                    if (code >= 500 || code == 429) {
                        SafeLog($"Backfill: Server Error {code} – Retry {attempt}/3. Body: {TrimForLog(body)}");
                        await Task.Delay(TimeSpan.FromMilliseconds(2000 * attempt), ct); // Noch längeres Delay
                        continue;
                    }

                    // andere Codes: protokollieren und zurück
                    SafeLog($"Backfill: Fehler {code} :: {TrimForLog(body)}");
                    return new List<Candle>();
                } catch (Exception ex) when (attempt < 3) {
                    SafeLog($"Backfill: Exception (Versuch {attempt}/3) – {ex.Message}");
                    await Task.Delay(TimeSpan.FromMilliseconds(1000 * attempt), ct);
                } catch (Exception ex) {
                    SafeLog($"BackfillWorker: HMDS EX – {ex.Message}");
                    break;
                }
            }
            // Fallback: leer
            SafeLog("BackfillWorker: HMDS leer nach Retries.");
            return new List<Candle>();
        }

        // ------------------------------ JSON Helpers ------------------------------
        private static readonly JsonSerializerOptions JsonOpts =
            new(JsonSerializerDefaults.Web) {
                PropertyNameCaseInsensitive = true,
                NumberHandling = JsonNumberHandling.AllowReadingFromString
            };

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
            if (!TryGetDouble(el, "o", out var o)) return false;
            if (!TryGetDouble(el, "h", out var h)) return false;
            if (!TryGetDouble(el, "l", out var l)) return false;
            if (!TryGetDouble(el, "c", out var close)) return false;

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
                if (DateTime.TryParse(s, CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out tsUtc))
                    return true;

                if (DateTime.TryParseExact(s, "yyyyMMdd-HH:mm:ss", CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal, out var a)) {
                    tsUtc = DateTime.SpecifyKind(a, DateTimeKind.Utc);
                    return true;
                }
                if (DateTime.TryParseExact(s, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal, out var b)) {
                    tsUtc = DateTime.SpecifyKind(b, DateTimeKind.Utc);
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
