using System;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MyBase.Data;
using MyBase.Models.Finance;

namespace MyBase.Services.MarketData {
    /// <summary>
    /// Realtime-Marketdata via IBKR Client Portal API (Snapshot-Polling 1 Hz).
    /// - Gate über AppSetting "MarketDataDesiredState" + SessionManager.
    /// - Aggregiert Ticks via MinuteBarBuilder zu 1m-Bars inkl. SpreadAvg/SpreadMax.
    /// - Pflegt FeedState (Status, Heartbeat, LastRealtimeTsUtc).
    /// </summary>
    public sealed class MarketDataService : BackgroundService {
        private readonly IServiceProvider _sp;
        private readonly ILogger<MarketDataService> _log;
        private readonly IHttpClientFactory _http;
        private readonly SessionManager _session;
        private string _lastGateSig = "";
        private DateTime _lastGateLogUtc = DateTime.MinValue;
        private readonly MinuteBarBuilder _barBuilder = new();

        // Snapshot-Logging-Drossel (beibehalten)
        private string _lastSnapshotSig = "";
        private string _lastSnapshotMinuteKey = "";

        // Heartbeat-Persist-Drossel
        private DateTime _lastHeartbeatPersistUtc = DateTime.MinValue;

        public MarketDataService(
            IServiceProvider sp,
            ILogger<MarketDataService> log,
            IHttpClientFactory http,
            SessionManager sessionManager) {
            _sp = sp;
            _log = log;
            _http = http;
            _session = sessionManager;
        }
        private void LogGateBlockedThrottled(string? desired, SessionState session) {
            var sig = $"{desired ?? "Stopped"}|{session}";
            var now = DateTime.UtcNow;
            if (sig != _lastGateSig || (now - _lastGateLogUtc) > TimeSpan.FromSeconds(30)) {
                _lastGateSig = sig;
                _lastGateLogUtc = now;
                SessionLogBuffer.Info("RT", "GATE", "blocked",
                    ("desired", desired ?? "Stopped"),
                    ("session", session.ToString()));
            }
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
            SessionLogBuffer.Info("RT", "START", "MarketDataService gestartet");

            while (!stoppingToken.IsCancellationRequested) {
                try {
                    using var scope = _sp.CreateScope();
                    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                    // Desired-State
                    var desired = await db.AppSettings
                        .AsNoTracking()
                        .Where(s => s.Key == "MarketDataDesiredState")
                        .Select(s => s.Value)
                        .FirstOrDefaultAsync(stoppingToken);

                    var isRunningDesired = string.Equals(desired, "Running", StringComparison.OrdinalIgnoreCase);
                    //if (!isRunningDesired || _session.State != SessionState.Connected) {
                    //    SessionLogBuffer.Info("RT", "GATE", "blocked",
                    //        ("desired", desired ?? "Stopped"),
                    //        ("session", _session.State.ToString()));
                    //    await Task.Delay(1000, stoppingToken);
                    //    continue;
                    //}
                    if (!isRunningDesired || _session.State != SessionState.Connected) {
                        LogGateBlockedThrottled(desired, _session.State);
                        await Task.Delay(1000, stoppingToken);
                        continue;
                    }


                    // aktives Instrument
                    var inst = await db.Instruments.AsNoTracking()
                        .Where(i => i.IsActive)
                        .OrderBy(i => i.Id)
                        .FirstOrDefaultAsync(stoppingToken);

                    if (inst is null) {
                        SessionLogBuffer.Warn("RT", "NO_INSTRUMENT", "kein aktives Instrument (IsActive=true erwartet)");
                        await Task.Delay(1000, stoppingToken);
                        continue;
                    }

                    // Snapshot-Loop
                    await RunSnapshotLoopAsync(inst, stoppingToken);
                } catch (OperationCanceledException) {
                    // shutdown
                } catch (Exception ex) {
                    SessionLogBuffer.Error("RT", "EX", "ExecuteAsync exception", ("err", ex.Message));
                    _log.LogError(ex, "MarketDataService.ExecuteAsync");
                    await Task.Delay(1000, stoppingToken);
                }
            }

            SessionLogBuffer.Info("RT", "STOP", "MarketDataService gestoppt");
        }

        private async Task RunSnapshotLoopAsync(Instrument inst, CancellationToken ct) {
            SessionLogBuffer.Info("RT", "SNAP_START", "start snapshot poll", ("symbol", inst.Symbol), ("conid", inst.IbConId));

            // FeedState -> Running + initialer Heartbeat + Fehler löschen
            await UpdateFeedStateAsync(inst.Id, status: 1, heartbeatUtc: DateTime.UtcNow, clearError: true);

            var client = _http.CreateClient("Cpapi");

            while (!ct.IsCancellationRequested && _session.State == SessionState.Connected) {
                try {
                    // Snapshot (31=Last, 84=BidPrice, 86=AskPrice, 87=Day Volume)
                    var url = $"/v1/api/iserver/marketdata/snapshot?conids={inst.IbConId}&fields=31,84,86,87";
                    using var r = await client.GetAsync(url, ct);
                    var body = await r.Content.ReadAsStringAsync(ct);

                    if (!r.IsSuccessStatusCode) {
                        SessionLogBuffer.Info("RT", "SNAP_MISS", "snapshot http",
                            ("code", (int)r.StatusCode), ("sample", Trim(body)));
                    } else {
                        if (TryParseSnapshot(body,
                                out decimal? last,
                                out long? totalVol,
                                out string lastField,
                                out decimal? bid,
                                out decimal? ask)) {
                            ThrottledSnapshotLog(lastField, last, totalVol);

                            var nowUtc = DateTime.UtcNow;
                            // Tick in den Minuten-Builder (inkl. Bid/Ask für Spread)
                            var finished = _barBuilder.PushTick(nowUtc, last!.Value, totalVol, bid, ask);

                            if (finished != null) {
                                var (tsMinuteUtc, O, H, L, C, V, sprAvg, sprMax) = finished.Value;
                                await PersistBarAsync(inst.Id, tsMinuteUtc, O, H, L, C, V, sprAvg, sprMax);
                            }
                        } else {
                            ThrottledSnapshotLog("-", null, null);
                        }
                    }
                } catch (OperationCanceledException) {
                    // shutdown
                } catch (Exception ex) {
                    SessionLogBuffer.Error("RT", "SNAP_EX", "snapshot exception", ("err", ex.Message));
                    _log.LogError(ex, "Snapshot loop");
                }

                // Heartbeat gedrosselt persistieren
                var now = DateTime.UtcNow;
                if ((now - _lastHeartbeatPersistUtc) > TimeSpan.FromSeconds(15)) {
                    await UpdateFeedStateAsync(inst.Id, heartbeatUtc: now);
                    _lastHeartbeatPersistUtc = now;
                }

                await Task.Delay(1000, ct);
            }

            // FeedState -> Stopped
            await UpdateFeedStateAsync(inst.Id, status: 0);
            SessionLogBuffer.Info("RT", "SNAP_STOP", "snapshot poll stopped");
        }

        // ---------- Persistierung ----------
        private async Task PersistBarAsync(int instrumentId, DateTime tsMinuteUtc,
            decimal O, decimal H, decimal L, decimal C, long V,
            decimal? spreadAvg, decimal? spreadMax) {
            using var scope = _sp.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var tsNy = IctTime.ToNy(tsMinuteUtc);
            var session = IctTime.SessionOf(tsNy);   // 1=RTH, 0=ETH
            var kz = IctTime.KillzoneOf(tsNy);

            // „stille“ ETH-Minuten verwerfen (bestehende Logik)
            bool isSilentEth = (session == 0 && V == 0 && O == H && H == L && L == C);
            if (isSilentEth) {
                SessionLogBuffer.Debug("RT", "BAR_SKIP_SILENT", "silent ETH minute",
                    ("tsUtc", tsMinuteUtc.ToString("u")), ("O", O), ("H", H), ("L", L), ("C", C));
                return;
            }

            // UPSERT
            var bar = await db.Bars1m.FindAsync(new object?[] { instrumentId, tsMinuteUtc });
            if (bar is null) {
                bar = new Bar1m {
                    InstrumentId = instrumentId,
                    TsUtc = tsMinuteUtc,
                    TsNy = tsNy,
                    Open = O,
                    High = H,
                    Low = L,
                    Close = C,
                    Volume = V,
                    Session = (byte)session,
                    Killzone = (byte)kz,
                    IngestSource = "realtime",
                    SpreadAvg = spreadAvg,
                    SpreadMax = spreadMax
                };
                db.Bars1m.Add(bar);

                SessionLogBuffer.Info("RT", "BAR_INSERT", "bar1m insert",
                    ("tsUtc", tsMinuteUtc.ToString("u")), ("O", O), ("H", H), ("L", L), ("C", C), ("V", V),
                    ("sess", session), ("kz", kz), ("sprAvg", spreadAvg ?? -1), ("sprMax", spreadMax ?? -1));
            } else {
                bar.TsNy = tsNy;
                bar.Open = O; bar.High = H; bar.Low = L; bar.Close = C;
                bar.Volume = V;
                bar.Session = (byte)session;
                bar.Killzone = (byte)kz;
                bar.IngestSource = "realtime";
                bar.SpreadAvg = spreadAvg;
                bar.SpreadMax = spreadMax;

                SessionLogBuffer.Info("RT", "BAR_UPDATE", "bar1m update",
                    ("tsUtc", tsMinuteUtc.ToString("u")), ("O", O), ("H", H), ("L", L), ("C", C), ("V", V),
                    ("sess", session), ("kz", kz), ("sprAvg", spreadAvg ?? -1), ("sprMax", spreadMax ?? -1));
            }

            // Guard kurz vor Save (bestehende Logik)
            if (session == 0 && bar.Volume == 0 && bar.Open == bar.High && bar.High == bar.Low && bar.Low == bar.Close) {
                SessionLogBuffer.Debug("RT", "BAR_GUARD", "blocked silent ETH minute before save",
                    ("tsUtc", tsMinuteUtc.ToString("u")));
                return;
            }

            await db.SaveChangesAsync();

            // FeedState: LastRealtimeTsUtc aktualisieren (bestehend)
            var fs = await db.FeedStates.FindAsync(instrumentId) ?? new FeedState { InstrumentId = instrumentId };
            fs.LastRealtimeTsUtc = tsMinuteUtc;
            fs.Status = 1;
            db.Update(fs);
            await db.SaveChangesAsync();
        }

        // ---------- Parser & Hilfen ----------
        private static bool TryParseSnapshot(string json,
            out decimal? last, out long? totalVol, out string lastField,
            out decimal? bid, out decimal? ask) {
            last = null; totalVol = null; lastField = "-";
            bid = null; ask = null;

            try {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return false;
                if (doc.RootElement.GetArrayLength() == 0) return false;

                var el = doc.RootElement[0];

                // 31 = Last
                if (el.TryGetProperty("31", out var f31) && TryGetDecimalFromFieldNode(f31, out var v31)) {
                    last = v31; lastField = "31";
                }

                // 87 = Tages-Volumen (kumuliert)
                if (el.TryGetProperty("87", out var f87) && TryGetLongFromFieldNode(f87, out var v87)) {
                    totalVol = v87;
                }

                // 84 = Bid Price, 86 = Ask Price
                if (el.TryGetProperty("84", out var f84) && TryGetDecimalFromFieldNode(f84, out var v84)) bid = v84;
                if (el.TryGetProperty("86", out var f86) && TryGetDecimalFromFieldNode(f86, out var v86)) ask = v86;

                // Fallback: "last"
                if (!last.HasValue && el.TryGetProperty("last", out var ln) && ln.TryGetDecimal(out var lv)) {
                    last = lv; lastField = "last";
                }

                return last.HasValue;
            } catch {
                return false;
            }
        }

        private static bool TryGetDecimalFromFieldNode(JsonElement node, out decimal v) {
            // kulturinvariant + unterstützt {value,scale} sowie {price:{value,scale}}
            if (TryParseScaledDecimal(node, out v)) return true;
            v = 0m; return false;
        }

        private static bool TryParseScaledDecimal(JsonElement node, out decimal v) {
            v = 0m;

            if (node.ValueKind == JsonValueKind.Number && node.TryGetDecimal(out v)) return true;

            if (node.ValueKind == JsonValueKind.String &&
                decimal.TryParse(node.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out v))
                return true;

            if (node.ValueKind == JsonValueKind.Object) {
                // price-Wrapper
                if (node.TryGetProperty("price", out var priceNode) && TryParseScaledDecimal(priceNode, out v))
                    return true;

                // {value, scale}
                if (node.TryGetProperty("value", out var valNode)) {
                    if (!TryParseScaledDecimal(valNode, out var raw)) return false;

                    int scale = 0;
                    if (node.TryGetProperty("scale", out var scNode)) {
                        if (scNode.ValueKind == JsonValueKind.Number && scNode.TryGetInt32(out var sc)) scale = sc;
                        else if (scNode.ValueKind == JsonValueKind.String && int.TryParse(scNode.GetString(), out sc)) scale = sc;
                    }

                    if (scale > 0) {
                        decimal pow = 1m;
                        for (int i = 0; i < scale; i++) pow *= 10m;
                        v = raw / pow;
                    } else {
                        v = raw;
                    }
                    return true;
                }

                // generischer Fallback: erstes parsebares Property
                foreach (var p in node.EnumerateObject()) {
                    if (TryParseScaledDecimal(p.Value, out v)) return true;
                }
            }

            return false;
        }

        private static bool TryGetLongFromFieldNode(JsonElement node, out long v) {
            v = 0;

            if (node.ValueKind == JsonValueKind.Number && node.TryGetInt64(out v)) return true;

            if (node.ValueKind == JsonValueKind.String &&
                long.TryParse(node.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out v))
                return true;

            if (node.ValueKind == JsonValueKind.Object && node.TryGetProperty("value", out var val)) {
                if (val.ValueKind == JsonValueKind.Number && val.TryGetInt64(out v)) return true;
                if (val.ValueKind == JsonValueKind.String &&
                    long.TryParse(val.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out v))
                    return true;
            }

            return false;
        }

        private void ThrottledSnapshotLog(string lastField, decimal? last, long? totalVol) {
            // Signatur + Minutenschlüssel (deine bestehende Drossel-Logik bleibt erhalten)
            var minuteKey = $"{DateTime.UtcNow:HH:mm}";
            var sig = $"{lastField}:{(last.HasValue ? last.Value.ToString(CultureInfo.InvariantCulture) : "--")}:{(totalVol.HasValue ? totalVol.Value.ToString(CultureInfo.InvariantCulture) : "--")}";

            if (minuteKey != _lastSnapshotMinuteKey || sig != _lastSnapshotSig) {
                _lastSnapshotMinuteKey = minuteKey;
                _lastSnapshotSig = sig;

                SessionLogBuffer.Info("RT", "HEARTBEAT", "minute",
                    ("key", minuteKey),
                    ("last", last.HasValue ? last.Value.ToString(CultureInfo.InvariantCulture) : "--"),
                    ("totalVol", totalVol.HasValue ? totalVol.Value.ToString(CultureInfo.InvariantCulture) : "--"));
            }
        }

        private async Task UpdateFeedStateAsync(int instrumentId, byte? status = null, DateTime? heartbeatUtc = null, string? error = null, bool clearError = false) {
            using var scope = _sp.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var fs = await db.FeedStates.FindAsync(instrumentId) ?? new FeedState { InstrumentId = instrumentId };
            if (status.HasValue) fs.Status = status.Value;
            if (heartbeatUtc.HasValue) fs.LastHeartbeatUtc = heartbeatUtc;
            
            if (clearError) fs.LastError = null;
            else if (error != null) fs.LastError = error;

            db.Update(fs);
            await db.SaveChangesAsync();
        }

        private static string Trim(string s) =>
            string.IsNullOrWhiteSpace(s) ? "" : (s.Length <= 200 ? s : s[..200] + " …");
    }
}
