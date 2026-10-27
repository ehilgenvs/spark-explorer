using System.Collections.Concurrent;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using SparkExplorer.Sparkplug;

namespace SparkExplorer.Services;

/// <summary>
/// One MQTT connection plus everything received on it. Scoped per Blazor circuit (browser tab).
/// Network callbacks only enqueue; all state changes happen in <see cref="Drain"/>, which the UI
/// calls on its own synchronization context, so rendering never races with incoming messages.
/// </summary>
public sealed class MqttSession : IAsyncDisposable
{
    private const int QueueLimit = 200_000;
    private const int SeriesLimit = 10_000;

    private readonly ILogger<MqttSession> _log;
    private readonly BrokerStore _store;
    private volatile string? _lastTlsError;
    private readonly ConcurrentQueue<Incoming> _queue = new();
    private readonly SparkplugDecoder _decoder = new();
    private readonly object _demoLock = new();

    private IMqttClient? _client;
    private MqttClientOptions? _options;
    private ConnectionSettings? _active;
    private volatile bool _userDisconnect;
    private volatile bool _statusChanged;
    private int _reconnecting;
    private System.Threading.Timer? _demoTimer;
    private DemoGenerator? _demo;
    private long _dropped;
    private int _rateCount;
    private DateTime _rateStart = DateTime.UtcNow;

    public MqttSession(BrokerStore store, ILogger<MqttSession> log)
    {
        _log = log;
        _store = store;
        Settings = store.All.FirstOrDefault() ?? new ConnectionSettings();
    }

    /// <summary>The broker profile used by the next Connect (a working copy of a saved broker).</summary>
    public ConnectionSettings Settings { get; set; }

    /// <summary>Raised (on any thread) when the connection state or status text changes.</summary>
    public event Action? StatusChanged;
    public ConnState State { get; private set; } = ConnState.Disconnected;
    public string StatusText { get; private set; } = "Disconnected";
    public bool IsConnected => _client?.IsConnected == true;
    public bool DemoRunning => _demoTimer != null;

    public Dictionary<string, TopicData> Topics { get; } = new();
    public TreeNode Root { get; } = new("", "");
    public Dictionary<string, SpNodeState> SpNodes { get; } = new();
    public Dictionary<string, SpHostState> SpHosts { get; } = new();
    public Dictionary<string, DataSeries> Series { get; } = new();
    /// <summary>Keys of the data points shown in the chart, in selection order.</summary>
    public List<string> Plotted { get; } = new();
    /// <summary>Raised on the UI thread when the plotted set changes.</summary>
    public event Action? PlotsChanged;
    public long TotalMessages { get; private set; }
    public int Rate { get; private set; }
    public long Dropped => Interlocked.Read(ref _dropped);

    // ------------------------------------------------------------------ connection

    public async Task ConnectAsync()
    {
        await DisconnectAsync();
        _userDisconnect = false;
        _lastTlsError = null;

        var s = Settings.Clone();
        MqttClientOptions options;
        try
        {
            options = BuildOptions(s);
        }
        catch (Exception ex)
        {
            var message = ex.Message;
            if (!s.SavePasswords && string.IsNullOrEmpty(s.Tls?.ClientCertificatePassword) && !string.IsNullOrWhiteSpace(s.Tls?.ClientCertificatePath))
                message += " This broker's passwords aren't saved: open its settings and enter the certificate password.";
            SetStatus(ConnState.Error, message);
            return;
        }

        var client = new MqttFactory().CreateMqttClient();
        _client = client;
        _options = options;
        _active = s;

        client.ApplicationMessageReceivedAsync += e =>
        {
            if (_queue.Count >= QueueLimit)
            {
                Interlocked.Increment(ref _dropped);
                return Task.CompletedTask;
            }
            var m = e.ApplicationMessage;
            _queue.Enqueue(new Incoming(m.Topic, m.PayloadSegment.ToArray(), (int)m.QualityOfServiceLevel, m.Retain, DateTime.Now));
            return Task.CompletedTask;
        };

        client.ConnectedAsync += e =>
        {
            SetStatus(ConnState.Connected, $"Connected to {s.Name} ({s.Describe()})");
            _ = Task.Run(() => SubscribeAsync(client, s));
            return Task.CompletedTask;
        };

        client.DisconnectedAsync += e =>
        {
            if (_client != client || _userDisconnect || !e.ClientWasConnected) return Task.CompletedTask;
            var why = e.Exception != null ? Innermost(e.Exception).Message : e.Reason.ToString();
            SetStatus(ConnState.Reconnecting, $"Connection lost ({why}), retrying…");
            _ = ReconnectLoopAsync(client);
            return Task.CompletedTask;
        };

        SetStatus(ConnState.Connecting, $"Connecting to {s.Name} ({s.Describe()})…");
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await client.ConnectAsync(options, cts.Token);
        }
        catch (Exception ex)
        {
            if (_client != client) return;
            _log.LogWarning(ex, "MQTT connect to {Broker} failed", s.Describe());
            var message = _lastTlsError ?? Innermost(ex).Message;
            // A broker that requires a client certificate just drops the TLS connection without saying why.
            if (_lastTlsError == null && s.UseTls && string.IsNullOrWhiteSpace(s.Tls.ClientCertificatePath) && ex is not MQTTnet.Adapter.MqttConnectingFailedException)
                message += ". If this broker requires a client certificate, add one in the broker's TLS settings.";
            if (ex is MQTTnet.Adapter.MqttConnectingFailedException && !string.IsNullOrEmpty(s.Username) && string.IsNullOrEmpty(s.Password) && !s.SavePasswords)
                message += ". This broker's password isn't saved: open its settings and enter it.";
            SetStatus(ConnState.Error, "Connect failed: " + message);
            _client = null;
            client.Dispose();
        }
    }

    public async Task DisconnectAsync()
    {
        _userDisconnect = true;
        var c = _client;
        _client = null;
        if (c != null)
        {
            try
            {
                if (c.IsConnected) await c.DisconnectAsync(new MqttClientDisconnectOptions(), CancellationToken.None);
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "Error while disconnecting");
            }
            c.Dispose();
        }
        if (State != ConnState.Disconnected) SetStatus(ConnState.Disconnected, "Disconnected");
    }

    private MqttClientOptions BuildOptions(ConnectionSettings s)
    {
        var b = new MqttClientOptionsBuilder()
            .WithClientId(string.IsNullOrWhiteSpace(s.ClientId) ? "spark-explorer-" + Guid.NewGuid().ToString("N")[..6] : s.ClientId.Trim())
            .WithCleanSession()
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
            .WithTimeout(TimeSpan.FromSeconds(10))
            .WithProtocolVersion(s.ProtocolVersion == 5 ? MqttProtocolVersion.V500 : MqttProtocolVersion.V311);

        var useTls = s.UseTls;
        if (s.Transport == TransportKind.Tcp)
        {
            if (string.IsNullOrWhiteSpace(s.Host)) throw new ArgumentException("Enter the broker host name or IP address.");
            if (s.Port is < 1 or > 65535) throw new ArgumentException("Enter a port between 1 and 65535.");
            b.WithTcpServer(s.Host.Trim(), s.Port);
        }
        else
        {
            var uri = s.WebSocketUri.Trim();
            if (!Uri.TryCreate(uri, UriKind.Absolute, out var parsed) || (parsed.Scheme != "ws" && parsed.Scheme != "wss"))
                throw new ArgumentException("Enter a WebSocket URL starting with ws:// or wss://.");
            useTls |= parsed.Scheme == "wss";
            b.WithWebSocketServer(o => o.WithUri(uri));
        }

        if (useTls)
        {
            var tls = s.Tls ?? new TlsSettings();
            var ca = string.IsNullOrWhiteSpace(tls.CaCertificatePath)
                ? null
                : CertificateFiles.LoadCaCertificates(_store.ResolvePath(tls.CaCertificatePath)!);
            var client = string.IsNullOrWhiteSpace(tls.ClientCertificatePath)
                ? null
                : CertificateFiles.LoadClientCertificate(_store.ResolvePath(tls.ClientCertificatePath)!,
                    _store.ResolvePath(tls.ClientKeyPath), tls.ClientCertificatePassword);

            b.WithTlsOptions(o =>
            {
                o.UseTls();
                // One handler for TCP and WebSocket (MQTTnet applies a custom trust chain only on TCP).
                o.WithCertificateValidationHandler(e => ValidateServerCertificate(e, tls, ca));
                if (client != null) o.WithClientCertificates(new X509Certificate2Collection(client));
                if (!tls.CheckRevocation) o.WithIgnoreCertificateRevocationErrors().WithRevocationMode(X509RevocationMode.NoCheck);
                if (tls.Version == "Tls12") o.WithSslProtocols(SslProtocols.Tls12);
                else if (tls.Version == "Tls13") o.WithSslProtocols(SslProtocols.Tls13);
                if (!string.IsNullOrWhiteSpace(tls.ServerName)) o.WithTargetHost(tls.ServerName.Trim());
            });
        }

        if (!string.IsNullOrEmpty(s.Username)) b.WithCredentials(s.Username, s.Password ?? "");
        return b.Build();
    }

    /// <summary>
    /// Accepts certificates trusted by the system, or chaining to the configured CA certificate.
    /// The host name must always match unless untrusted certificates are allowed.
    /// </summary>
    private bool ValidateServerCertificate(MqttClientCertificateValidationEventArgs e, TlsSettings tls, X509Certificate2Collection? ca)
    {
        if (tls.AllowUntrustedCertificates) return true;
        var errors = e.SslPolicyErrors;
        if (errors == SslPolicyErrors.None) return true;
        if (errors.HasFlag(SslPolicyErrors.RemoteCertificateNotAvailable))
        {
            _lastTlsError = "The broker did not send a TLS certificate.";
            return false;
        }
        if (errors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch))
        {
            using var c = new X509Certificate2(e.Certificate);
            _lastTlsError = $"The broker's certificate is issued to \"{c.GetNameInfo(X509NameType.DnsName, false)}\", not the address you connected to. "
                + "Connect using that name, or enter it as Server name in the TLS settings.";
            return false;
        }

        using var cert = new X509Certificate2(e.Certificate);
        using var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = tls.CheckRevocation ? X509RevocationMode.Online : X509RevocationMode.NoCheck;
        if (ca is { Count: > 0 })
        {
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.AddRange(ca);
            chain.ChainPolicy.ExtraStore.AddRange(ca);
        }
        if (e.Chain != null)
            foreach (var element in e.Chain.ChainElements) chain.ChainPolicy.ExtraStore.Add(element.Certificate);
        if (chain.Build(cert)) return true;

        var reasons = string.Join("; ", chain.ChainStatus.Select(st => st.StatusInformation.Trim().TrimEnd('.')).Where(r => r.Length > 0).Distinct());
        _lastTlsError = $"The broker's certificate is not trusted ({reasons}). "
            + (ca == null ? "Add the CA certificate that signed it" : "Check that the CA certificate is the one that signed the broker's certificate")
            + ", or allow untrusted certificates for testing.";
        return false;
    }

    private async Task SubscribeAsync(IMqttClient client, ConnectionSettings s)
    {
        var filters = s.Subscriptions.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (filters.Length == 0) filters = new[] { "#" };
        var qos = (MqttQualityOfServiceLevel)Math.Clamp(s.SubscribeQos, 0, 2);

        var builder = new MqttClientSubscribeOptionsBuilder();
        foreach (var f in filters) builder.WithTopicFilter(t => t.WithTopic(f).WithQualityOfServiceLevel(qos));

        try
        {
            var result = await client.SubscribeAsync(builder.Build());
            var rejected = result.Items.Where(i => (int)i.ResultCode > 2).Select(i => i.TopicFilter.Topic).ToList();
            SetStatus(ConnState.Connected, rejected.Count == 0
                ? $"Connected to {s.Name} ({s.Describe()})"
                : $"Connected to {s.Name}; broker refused {string.Join(", ", rejected)}");
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Subscribe failed");
            SetStatus(ConnState.Error, "Subscribe failed: " + Innermost(ex).Message);
        }
    }

    private async Task ReconnectLoopAsync(IMqttClient client)
    {
        if (Interlocked.Exchange(ref _reconnecting, 1) == 1) return;
        try
        {
            while (!_userDisconnect && _client == client && !client.IsConnected)
            {
                await Task.Delay(3000);
                if (_userDisconnect || _client != client) break;
                try
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                    await client.ConnectAsync(_options!, cts.Token);
                }
                catch (Exception ex)
                {
                    SetStatus(ConnState.Reconnecting, $"Reconnect to {_active?.Describe()} failed ({Innermost(ex).Message}), retrying…");
                }
            }
        }
        finally
        {
            Interlocked.Exchange(ref _reconnecting, 0);
        }
    }

    public async Task<string?> PublishAsync(string topic, byte[] payload, int qos, bool retain)
    {
        var c = _client;
        if (c is not { IsConnected: true }) return "Not connected to a broker.";
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)Math.Clamp(qos, 0, 2))
            .WithRetainFlag(retain)
            .Build();
        try
        {
            var r = await c.PublishAsync(message);
            return r.IsSuccess ? null : $"Broker answered {r.ReasonCode} {r.ReasonString}".Trim();
        }
        catch (Exception ex)
        {
            return Innermost(ex).Message;
        }
    }

    /// <summary>Sends an NCMD with Node Control/Rebirth = true so the edge node republishes its births.</summary>
    public Task<string?> SendRebirthAsync(string group, string edgeNode)
    {
        var now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var p = new PbPayload { Timestamp = now };
        p.Metrics.Add(new PbMetric
        {
            Name = "Node Control/Rebirth",
            Datatype = SpDataTypes.Boolean,
            Timestamp = now,
            Value = PbValue.OfBool(true),
        });
        return PublishAsync($"spBv1.0/{group}/NCMD/{edgeNode}", SparkplugCodec.Encode(p), 0, false);
    }

    private void SetStatus(ConnState state, string text)
    {
        State = state;
        StatusText = text;
        _statusChanged = true;
        StatusChanged?.Invoke();
    }

    private static Exception Innermost(Exception ex)
    {
        while (ex.InnerException != null) ex = ex.InnerException;
        return ex;
    }

    // ------------------------------------------------------------------ demo traffic

    public void StartDemo()
    {
        lock (_demoLock)
        {
            if (_demoTimer != null) return;
            _demo = new DemoGenerator();
            foreach (var m in _demo.Births()) _queue.Enqueue(m);
            _demoTimer = new System.Threading.Timer(_ =>
            {
                lock (_demoLock)
                {
                    if (_demo == null) return;
                    foreach (var m in _demo.Tick()) _queue.Enqueue(m);
                }
            }, null, 0, 1000);
        }
    }

    public void StopDemo()
    {
        lock (_demoLock)
        {
            _demoTimer?.Dispose();
            _demoTimer = null;
            _demo = null;
        }
    }

    // ------------------------------------------------------------------ message store

    /// <summary>Applies queued messages. Call from the UI thread. Returns true when the UI should re-render.</summary>
    public bool Drain(int max = 20_000)
    {
        var n = 0;
        while (n < max && _queue.TryDequeue(out var item))
        {
            Process(item);
            n++;
        }

        var changed = n > 0;
        var now = DateTime.UtcNow;
        var elapsed = (now - _rateStart).TotalSeconds;
        if (elapsed >= 1)
        {
            Rate = (int)Math.Round(_rateCount / elapsed);
            _rateCount = 0;
            _rateStart = now;
            changed = true;
        }
        if (_statusChanged)
        {
            _statusChanged = false;
            changed = true;
        }
        return changed;
    }

    private void Process(Incoming item)
    {
        var msg = new MqttMessage { Received = item.Received, Payload = item.Payload, Qos = item.Qos, Retain = item.Retain };

        var sp = SpTopic.Parse(item.Topic);
        if (sp != null)
        {
            try
            {
                msg.Sparkplug = _decoder.Decode(item.Payload, sp, track: true);
                TrackSparkplug(sp, msg.Sparkplug, item.Received);
            }
            catch (Exception ex)
            {
                msg.SparkplugError = ex.Message;
            }
        }
        else if (SpTopic.ParseStateHost(item.Topic) is { } host)
        {
            SpHosts[host] = new SpHostState(ParseHostOnline(msg.Text), item.Received);
        }

        RecordSeries(item.Topic, msg);

        var isNew = !Topics.TryGetValue(item.Topic, out var td);
        if (td is null)
        {
            td = new TopicData { Topic = item.Topic };
            Topics[item.Topic] = td;
        }
        td.History.Insert(0, msg);
        if (td.History.Count > TopicData.HistoryLimit) td.History.RemoveAt(td.History.Count - 1);
        td.Count++;
        TotalMessages++;
        _rateCount++;

        var node = Root;
        node.MessageCount++;
        if (isNew) node.TopicCount++;
        var parts = item.Topic.Split('/');
        for (var i = 0; i < parts.Length; i++)
        {
            node = node.GetOrAdd(parts[i], i == parts.Length - 1 ? item.Topic : string.Join('/', parts, 0, i + 1));
            node.MessageCount++;
            if (isNew) node.TopicCount++;
        }
        node.Topic = td;
    }

    // ------------------------------------------------------------------ data point history

    public bool IsPlotted(string key) => Plotted.Contains(key);

    public void TogglePlot(string key)
    {
        if (!Plotted.Remove(key)) Plotted.Add(key);
        PlotsChanged?.Invoke();
    }

    public void ClearPlots()
    {
        Plotted.Clear();
        PlotsChanged?.Invoke();
    }

    private void RecordSeries(string topic, MqttMessage msg)
    {
        if (msg.Sparkplug is { } sp)
        {
            var spTopic = SpTopic.Parse(topic)!;
            if (!spTopic.IsBirth && !spTopic.IsData) return;
            foreach (var m in sp.Metrics)
            {
                if (m.Name == null || SeriesKeys.ToDouble(m.Value) is not { } v) continue;
                var series = GetSeries(SeriesKeys.ForMetric(spTopic.Key, m.Name), m.Name, spTopic.Key);
                if (series == null) continue;
                if (m.Properties != null && m.Properties.TryGetValue("engUnit", out var unit) && unit is string u) series.Unit = u;
                var time = (m.Timestamp ?? sp.Timestamp)?.LocalDateTime ?? msg.Received;
                series.Add(time, v);
            }
            return;
        }

        if (msg.Text.Kind != PayloadKind.Json || msg.Text.Json is not { } json) return;
        foreach (var (path, value) in JsonPoints.Extract(json))
        {
            var label = path.Length == 0 ? topic.Split('/')[^1] : path;
            GetSeries(SeriesKeys.ForTopic(topic, path), label, topic)?.Add(msg.Received, value);
        }
    }

    private DataSeries? GetSeries(string key, string label, string source)
    {
        if (Series.TryGetValue(key, out var s)) return s;
        if (Series.Count >= SeriesLimit && !Plotted.Contains(key)) return null;
        s = new DataSeries { Key = key, Label = label, Source = source };
        Series[key] = s;
        return s;
    }

    private static bool? ParseHostOnline(PayloadText t)
    {
        if (t.Kind == PayloadKind.Json && t.Json is { ValueKind: JsonValueKind.Object } j
            && j.TryGetProperty("online", out var online) && online.ValueKind is JsonValueKind.True or JsonValueKind.False)
            return online.GetBoolean();
        if (t.Kind == PayloadKind.Text)
        {
            var s = t.Text.Trim();
            if (s.Equals("ONLINE", StringComparison.OrdinalIgnoreCase)) return true;
            if (s.Equals("OFFLINE", StringComparison.OrdinalIgnoreCase)) return false;
        }
        return null;
    }

    private SpNodeState GetSpNode(string group, string edge, string? device)
    {
        var key = device == null ? $"{group}/{edge}" : $"{group}/{edge}/{device}";
        if (!SpNodes.TryGetValue(key, out var n))
        {
            n = new SpNodeState { Key = key, Group = group, EdgeNode = edge, Device = device };
            SpNodes[key] = n;
        }
        return n;
    }

    private void TrackSparkplug(SpTopic sp, SpPayload payload, DateTime received)
    {
        if (sp.Device != null) GetSpNode(sp.Group, sp.EdgeNode, null);
        var e = GetSpNode(sp.Group, sp.EdgeNode, sp.Device);
        e.LastType = sp.MessageType;
        e.LastUpdate = received;
        e.Count++;
        if (payload.Seq != null) e.Seq = payload.Seq;

        if (sp.IsBirth)
        {
            e.Online = true;
            e.Metrics.Clear();
        }
        else if (sp.MessageType == "DDEATH")
        {
            e.Online = false;
        }
        else if (sp.MessageType == "NDEATH")
        {
            foreach (var n in SpNodes.Values)
                if (n.Group == sp.Group && n.EdgeNode == sp.EdgeNode) n.Online = false;
        }

        if (sp.IsBirth || sp.IsData)
        {
            foreach (var m in payload.Metrics)
            {
                var name = m.Name ?? $"alias {m.Alias}";
                e.Metrics[name] = new SpMetricState(name, m.TypeName, m.Value, m.Timestamp ?? payload.Timestamp ?? new DateTimeOffset(received));
            }
        }
    }

    /// <summary>Decodes a payload as Sparkplug B on request (e.g. for topics outside spBv1.0).</summary>
    public (SpPayload? Payload, string? Error) DecodeOnDemand(string topic, MqttMessage msg)
    {
        if (msg.Sparkplug != null) return (msg.Sparkplug, null);
        try
        {
            return (_decoder.Decode(msg.Payload, SpTopic.Parse(topic), track: false), null);
        }
        catch (Exception ex)
        {
            return (null, ex.Message);
        }
    }

    public void Clear()
    {
        Topics.Clear();
        Root.Clear();
        SpNodes.Clear();
        SpHosts.Clear();
        Series.Clear();
        Plotted.Clear();
        _decoder.Reset();
        TotalMessages = 0;
        Interlocked.Exchange(ref _dropped, 0);
        PlotsChanged?.Invoke();
    }

    public async ValueTask DisposeAsync()
    {
        StopDemo();
        await DisconnectAsync();
    }
}
