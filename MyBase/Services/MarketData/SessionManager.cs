using System.Net; // Für CookieContainer
using System.Net.Http.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using MyBase.Data;
using MyBase.Models.Finance;

namespace MyBase.Services.MarketData;

public class SessionManager : BackgroundService {
    private readonly IHttpClientFactory _httpFactory;
    private readonly IServiceProvider _sp;
    private readonly ILogger<SessionManager> _log;
    private readonly TimeSpan _heartbeat;
    private readonly TimeSpan _statusPoll;
    private readonly CookieContainer _cookies; // Store reference to clear cookies

    private SessionState _state = SessionState.Disconnected;
    public SessionState State => _state;

    public SessionManager(
        IHttpClientFactory httpFactory,
        IServiceProvider sp,
        ILogger<SessionManager> log,
        IOptionsMonitor<CpapiOptions> opt,
        CookieContainer cookies // Inject CookieContainer
    ) {
        _httpFactory = httpFactory;
        _sp = sp;
        _log = log;
        _cookies = cookies;

        var cfg = opt.CurrentValue;
        _heartbeat = TimeSpan.FromSeconds(cfg.HeartbeatSeconds);
        _statusPoll = TimeSpan.FromSeconds(cfg.StatusPollSeconds);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var client = _httpFactory.CreateClient("Cpapi");
        var lastTickle = DateTime.MinValue;
        var lastStatus = DateTime.MinValue;

        SessionLogBuffer.Info("SM", "START", "SessionManager gestartet", ("BaseUrl", client.BaseAddress?.ToString() ?? "null"));

        while (!stoppingToken.IsCancellationRequested) {
            try {
                var gwDesired = await GetGatewayDesiredAsync(stoppingToken);

                // Gateway gestoppt → keine Sessionaktivität
                if (gwDesired == "Stopped") {
                    if (_state != SessionState.Disconnected)
                        SessionLogBuffer.Info("SM", "GW_STOPPED", "GatewayDesired=Stopped", ("from", _state), ("to", SessionState.Disconnected));
                    _state = SessionState.Disconnected;
                    await Task.Delay(1000, stoppingToken);
                    continue;
                }

                // 1) SSO prüfen/verlängern
                var ssoOk = await EnsureSsoAsync(client, stoppingToken);
                
                // Wenn SSO fehlschlägt (z.B. 401), MÜSSEN wir ProbeAuthAsync aufrufen, damit Reauthenticate passiert!
                // Wir erzwingen den Check, wenn ssoOk false ist.
                
                var fastProbe = _state != SessionState.Connected || !ssoOk;
                var due = fastProbe ? TimeSpan.FromSeconds(5) : _statusPoll;

                if (!ssoOk || (DateTime.UtcNow - lastStatus) > due) {
                    if (!ssoOk && _state != SessionState.NeedsLogin) {
                         SessionLogBuffer.Warn("SM", "SSO", "invalid -> forcing auth probe", ("state", _state));
                         _state = SessionState.NeedsLogin;
                    }

                    var before = _state;
                    var next = await ProbeAuthAsync(client, stoppingToken);
                    if (next != before)
                        SessionLogBuffer.Info("SM", "STATE", "probe transition", ("from", before), ("to", next));
                    _state = next;
                    lastStatus = DateTime.UtcNow;
                }

                // 3) Heartbeat/Tickle senden, wenn verbunden
                if (_state == SessionState.Connected &&
                    (DateTime.UtcNow - lastTickle) > _heartbeat) {
                    await TickleAsync(client, stoppingToken);
                    lastTickle = DateTime.UtcNow;
                    await UpdateHeartbeatAsync();
                }

                await Task.Delay(1000, stoppingToken);
            } catch (OperationCanceledException) {
                // normal during shutdown
            } catch (Exception ex) {
                _log.LogError(ex, "Fehler im SessionManager");
                _state = SessionState.Error;
                await SetFeedLastErrorAsync(ex.Message);
                SessionLogBuffer.Error("SM", "EX", "exception", ("err", ex.Message));
                await Task.Delay(3000, stoppingToken);
            }
        }

        SessionLogBuffer.Info("SM", "STOP", "SessionManager gestoppt");
    }

    // ---------------- Helpers ----------------

    private async Task<string> GetGatewayDesiredAsync(CancellationToken ct) {
        using var scope = _sp.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var s = await db.AppSettings.FindAsync(new object?[] { "GatewayDesiredState" }, ct);
        return s?.Value ?? "Running";
    }

    private async Task<bool> EnsureSsoAsync(HttpClient c, CancellationToken ct) {
        try {
            // /v1/api/sso/validate (GET)
            var res = await c.GetAsync("/v1/api/sso/validate", ct);
            if (res.IsSuccessStatusCode) {
                SessionLogBuffer.Throttled("sso-ok", TimeSpan.FromSeconds(30),
                    LogLevel.Info, "SM", "SSO", "validate ok");
                return true;
            }
            if (res.StatusCode == System.Net.HttpStatusCode.Unauthorized) {
                SessionLogBuffer.Warn("SM", "SSO", "401 Unauthorized -> Triggering Re-Auth");
                // Wir können den Container nicht einfach leeren (readonly/singleton), 
                // aber wir können den State so setzen, dass Reauthenticate versucht wird.
                return false;
            }
            
            SessionLogBuffer.Warn("SM", "SSO", "validate http", ("code", (int)res.StatusCode));
            return false;
        } catch (Exception ex) {
            SessionLogBuffer.Error("SM", "SSO", "validate error", ("err", ex.Message));
            return false;
        }
    }

    // ProbeAuth mit Auto-Reauth + Accounts-Call
    private async Task<SessionState> ProbeAuthAsync(HttpClient c, CancellationToken ct) {
        var st = await GetAuthStatusAsync(c, ct);
        if (!st.ok) return _state; // transient → State behalten

        if (!st.authenticated) {
            var re = await ReauthenticateAsync(c, ct);
            if (re) {
                for (int i = 0; i < 5; i++) {
                    await Task.Delay(1000, ct);
                    st = await GetAuthStatusAsync(c, ct);
                    if (st.ok && st.authenticated) break;
                }
            }
        }

        // Wenn authenticated, aber noch nicht connected → Accounts anstoßen
        if (st.authenticated && !st.connected) {
            await EnsureConnectedAsync(c, ct);
            st = await GetAuthStatusAsync(c, ct);
        }

        if (!st.authenticated) return SessionState.NeedsLogin;
        return st.connected ? SessionState.Connected : SessionState.Connecting;
    }

    private async Task<(bool ok, bool authenticated, bool connected)> GetAuthStatusAsync(HttpClient c, CancellationToken ct) {
        try {
            var res = await c.GetAsync("/v1/api/iserver/auth/status", ct);
            if (!res.IsSuccessStatusCode) {
                SessionLogBuffer.Warn("SM", "AUTH", "http", ("code", (int)res.StatusCode));
                // FIX: Bei 401 (Unauthorized) müssen wir so tun, als wären wir nicht authentifiziert,
                // damit der Caller (ProbeAuthAsync) das Re-Auth triggert.
                if (res.StatusCode == System.Net.HttpStatusCode.Unauthorized) {
                    return (true, false, false); // "ok=true" damit wir das Ergebnis nutzen, "auth=false" damit Reauth passiert
                }
                return (false, false, false);
            }
            var json = await res.Content.ReadFromJsonAsync<AuthStatus>(cancellationToken: ct);
            if (json is null) {
                SessionLogBuffer.Warn("SM", "AUTH", "empty");
                return (false, false, false);
            }
            SessionLogBuffer.Throttled("auth-status", TimeSpan.FromSeconds(30),
                LogLevel.Info, "SM", "AUTH", "status",
                ("authenticated", json.authenticated), ("connected", json.connected));
            return (true, json.authenticated, json.connected);
        } catch (Exception ex) {
            SessionLogBuffer.Error("SM", "AUTH", "error", ("err", ex.Message));
            return (false, false, false);
        }
    }

    private async Task<bool> ReauthenticateAsync(HttpClient c, CancellationToken ct) {
        try {
            // IB erwartet POST; ein leerer Body hilft, Content-Length != 0 zu haben
            var res = await c.PostAsync("/v1/api/iserver/reauthenticate", JsonContent.Create(new { }), ct);
            SessionLogBuffer.Info("SM", "REAUTH", "reauth", ("http", (int)res.StatusCode), ("ok", res.IsSuccessStatusCode));
            return res.IsSuccessStatusCode;
        } catch (Exception ex) {
            SessionLogBuffer.Error("SM", "REAUTH", "error", ("err", ex.Message));
            return false;
        }
    }

    private async Task TickleAsync(HttpClient c, CancellationToken ct) {
        try {
            // GET /v1/api/tickle
            var res = await c.GetAsync("/v1/api/tickle", ct);
            SessionLogBuffer.Throttled("tickle", TimeSpan.FromSeconds(30),
                LogLevel.Info, "SM", "TICKLE", "tickle", ("http", (int)res.StatusCode), ("ok", res.IsSuccessStatusCode));
        } catch (Exception ex) {
            SessionLogBuffer.Error("SM", "TICKLE", "error", ("err", ex.Message));
        }
    }

    private async Task EnsureConnectedAsync(HttpClient c, CancellationToken ct) {
        try {
            var res = await c.GetAsync("/v1/api/iserver/accounts", ct);
            if (res.IsSuccessStatusCode) {
                var before = _state;
                _state = SessionState.Connected;
                SessionLogBuffer.Info("SM", "ACCOUNTS", "connected", ("from", before), ("to", _state));
            } else {
                SessionLogBuffer.Warn("SM", "ACCOUNTS", "http", ("code", (int)res.StatusCode));
            }
        } catch (Exception ex) {
            SessionLogBuffer.Error("SM", "ACCOUNTS", "error", ("err", ex.Message));
        }
    }

    private async Task UpdateHeartbeatAsync() {
        using var scope = _sp.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var inst = await db.Instruments.AsNoTracking()
            .Where(i => i.IsActive)
            .OrderBy(i => i.Id)
            .FirstOrDefaultAsync();

        if (inst is null) return;

        var fs = await db.FeedStates.FindAsync(inst.Id) ?? new FeedState { InstrumentId = inst.Id };
        fs.LastHeartbeatUtc = DateTime.UtcNow;

        db.Update(fs);
        await db.SaveChangesAsync();
    }

    private async Task SetFeedLastErrorAsync(string msg) {
        using var scope = _sp.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var inst = await db.Instruments.AsNoTracking()
            .Where(i => i.IsActive)
            .OrderBy(i => i.Id)
            .FirstOrDefaultAsync();

        if (inst is null) return;

        var fs = await db.FeedStates.FindAsync(inst.Id) ?? new FeedState { InstrumentId = inst.Id };
        fs.Status = (byte)FeedRunState.Error;
        fs.LastError = msg;

        db.Update(fs);
        await db.SaveChangesAsync();
    }

    private sealed class AuthStatus {
        public bool authenticated { get; set; }
        public bool connected { get; set; }
    }
}
