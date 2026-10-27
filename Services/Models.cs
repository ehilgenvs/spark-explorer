using System.Collections;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Components;
using SparkExplorer.Sparkplug;

namespace SparkExplorer.Services;

public enum TransportKind { Tcp, WebSocket }

public enum ConnState { Disconnected, Connecting, Connected, Reconnecting, Error }

public sealed class ConnectionSettings
{
    public TransportKind Transport { get; set; } = TransportKind.Tcp;
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1883;
    public string WebSocketUri { get; set; } = "ws://localhost:9001";
    public bool UseTls { get; set; }
    public bool AllowUntrustedCertificates { get; set; }
    public string ClientId { get; set; } = "spark-explorer-" + Guid.NewGuid().ToString("N")[..6];
    public string? Username { get; set; }
    public string? Password { get; set; }
    /// <summary>4 = MQTT 3.1.1, 5 = MQTT 5.0</summary>
    public int ProtocolVersion { get; set; } = 4;
    public string Subscriptions { get; set; } = "#, $SYS/#";
    public int SubscribeQos { get; set; }

    public ConnectionSettings Clone() => (ConnectionSettings)MemberwiseClone();

    public string Describe() => Transport == TransportKind.Tcp
        ? $"{(UseTls ? "mqtts" : "mqtt")}://{Host}:{Port}"
        : WebSocketUri;
}

public sealed record Incoming(string Topic, byte[] Payload, int Qos, bool Retain, DateTime Received);

public sealed class MqttMessage
{
    public required DateTime Received { get; init; }
    public required byte[] Payload { get; init; }
    public int Qos { get; init; }
    public bool Retain { get; init; }
    public SpPayload? Sparkplug { get; set; }
    public string? SparkplugError { get; set; }

    private PayloadText? _text;
    private string? _preview;

    public PayloadText Text => _text ??= PayloadText.From(Payload);

    public string Preview => _preview ??= MakePreview();

    private string MakePreview()
    {
        if (Sparkplug != null) return $"{Sparkplug.MessageType} – {Sparkplug.Metrics.Count} metric{(Sparkplug.Metrics.Count == 1 ? "" : "s")}";
        if (SparkplugError != null) return "Sparkplug decode error";
        var t = Text;
        var s = t.Kind switch
        {
            PayloadKind.Empty => "(empty)",
            PayloadKind.Binary => $"<{Payload.Length} bytes binary>",
            PayloadKind.Json => t.CompactJson!,
            _ => t.Text,
        };
        return s.Length > 90 ? s[..90] + "…" : s;
    }
}

public enum PayloadKind { Empty, Text, Json, Binary }

public sealed class PayloadText
{
    private static readonly UTF8Encoding Strict = new(false, true);

    public PayloadKind Kind { get; private init; }
    public string Text { get; private init; } = "";
    public string? PrettyJson { get; private set; }
    public string? CompactJson { get; private set; }
    public JsonElement? Json { get; private set; }

    public static PayloadText From(byte[] b)
    {
        if (b.Length == 0) return new PayloadText { Kind = PayloadKind.Empty };
        string s;
        try { s = Strict.GetString(b); }
        catch (DecoderFallbackException) { return new PayloadText { Kind = PayloadKind.Binary }; }

        foreach (var c in s)
            if (c < 32 && c != '\t' && c != '\n' && c != '\r') return new PayloadText { Kind = PayloadKind.Binary };

        var trimmed = s.Trim();
        if (trimmed.Length > 0 && (trimmed[0] is '{' or '[' or '"' or '-' || char.IsDigit(trimmed[0]) || trimmed is "true" or "false" or "null"))
        {
            try
            {
                using var doc = JsonDocument.Parse(trimmed);
                var r = new PayloadText { Kind = PayloadKind.Json, Text = s, Json = doc.RootElement.Clone() };
                r.PrettyJson = JsonSerializer.Serialize(doc.RootElement, Fmt.PrettyOptions);
                r.CompactJson = JsonSerializer.Serialize(doc.RootElement, Fmt.CompactOptions);
                return r;
            }
            catch (JsonException) { }
        }
        return new PayloadText { Kind = PayloadKind.Text, Text = s };
    }
}

public sealed class TopicData
{
    public const int HistoryLimit = 100;
    public required string Topic { get; init; }
    /// <summary>Newest first.</summary>
    public List<MqttMessage> History { get; } = new();
    public long Count { get; set; }
}

public sealed class TreeNode
{
    private List<TreeNode>? _sorted;

    public TreeNode(string name, string path)
    {
        Name = name;
        Path = path;
    }

    public string Name { get; }
    public string Path { get; }
    public Dictionary<string, TreeNode> Children { get; } = new();
    public TopicData? Topic { get; set; }
    public long MessageCount { get; set; }
    public int TopicCount { get; set; }

    // Used by the UI to replay the "value changed" highlight.
    public long SeenCount { get; set; } = -1;
    public bool FlashToggle { get; set; }

    public IReadOnlyList<TreeNode> SortedChildren =>
        _sorted ??= Children.Values.OrderBy(c => c.Name, NaturalComparer.Instance).ToList();

    public TreeNode GetOrAdd(string name, string path)
    {
        if (!Children.TryGetValue(name, out var c))
        {
            c = new TreeNode(name, path);
            Children[name] = c;
            _sorted = null;
        }
        return c;
    }

    public void Clear()
    {
        Children.Clear();
        _sorted = null;
        MessageCount = 0;
        TopicCount = 0;
    }
}

public sealed class SpNodeState
{
    public required string Key { get; init; }
    public required string Group { get; init; }
    public required string EdgeNode { get; init; }
    public string? Device { get; init; }
    public bool? Online { get; set; }
    public string? LastType { get; set; }
    public ulong? Seq { get; set; }
    public DateTime LastUpdate { get; set; }
    public long Count { get; set; }
    public Dictionary<string, SpMetricState> Metrics { get; } = new();
}

public sealed record SpMetricState(string Name, string TypeName, object? Value, DateTimeOffset? Timestamp);

public sealed record SpHostState(bool? Online, DateTime Updated);

public sealed class NaturalComparer : IComparer<string>
{
    public static readonly NaturalComparer Instance = new();

    public int Compare(string? a, string? b)
    {
        if (a is null || b is null) return string.CompareOrdinal(a, b);
        int i = 0, j = 0;
        while (i < a.Length && j < b.Length)
        {
            if (char.IsDigit(a[i]) && char.IsDigit(b[j]))
            {
                int si = i, sj = j;
                while (i < a.Length && char.IsDigit(a[i])) i++;
                while (j < b.Length && char.IsDigit(b[j])) j++;
                var na = a.AsSpan(si, i - si).TrimStart('0');
                var nb = b.AsSpan(sj, j - sj).TrimStart('0');
                if (na.Length != nb.Length) return na.Length.CompareTo(nb.Length);
                var c = na.CompareTo(nb, StringComparison.Ordinal);
                if (c != 0) return c;
            }
            else
            {
                var c = char.ToLowerInvariant(a[i]).CompareTo(char.ToLowerInvariant(b[j]));
                if (c != 0) return c;
                i++;
                j++;
            }
        }
        return (a.Length - i).CompareTo(b.Length - j);
    }
}

public static class Fmt
{
    public static readonly JsonSerializerOptions PrettyOptions = new()
    {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
    };

    public static readonly JsonSerializerOptions CompactOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
    };

    private static readonly Regex JsonToken = new(
        @"(&quot;(?:[^&]|&(?!quot;))*?&quot;)(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)",
        RegexOptions.Compiled);

    public static string Time(DateTime t) => t.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
    public static string Time(DateTimeOffset? t) => t?.ToLocalTime().ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) ?? "–";
    public static string Full(DateTime t) => t.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
    public static string Full(DateTimeOffset? t) => t?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture) ?? "–";

    public static string Ago(DateTime t)
    {
        if (t == default) return "–";
        var s = (DateTime.Now - t).TotalSeconds;
        return s < 2 ? "just now" : s < 60 ? $"{(int)s}s ago" : s < 3600 ? $"{(int)(s / 60)}m ago" : $"{(int)(s / 3600)}h ago";
    }

    public static string HexShort(byte[] b, int max = 48)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < Math.Min(b.Length, max); i++)
        {
            if (i > 0) sb.Append(' ');
            sb.Append(b[i].ToString("x2"));
        }
        if (b.Length > max) sb.Append($" … ({b.Length} bytes)");
        return sb.ToString();
    }

    public static string HexDump(byte[] b, int max = 8192)
    {
        if (b.Length == 0) return "(empty)";
        var sb = new StringBuilder();
        var n = Math.Min(b.Length, max);
        for (var o = 0; o < n; o += 16)
        {
            sb.Append(o.ToString("x6")).Append("  ");
            var hex = new StringBuilder();
            var ascii = new StringBuilder();
            for (var i = o; i < Math.Min(o + 16, n); i++)
            {
                hex.Append(b[i].ToString("x2")).Append(' ');
                ascii.Append(b[i] is >= 32 and < 127 ? (char)b[i] : '.');
            }
            sb.Append(hex.ToString().PadRight(48)).Append(' ').Append(ascii).Append('\n');
        }
        if (b.Length > max) sb.Append($"… {b.Length - max} more bytes");
        return sb.ToString();
    }

    public static string PrettyJson(object value) => JsonSerializer.Serialize(value, PrettyOptions);

    /// <summary>HTML-encodes JSON and wraps keys, strings, numbers and literals in spans for coloring.</summary>
    public static MarkupString HighlightJson(string json)
    {
        var encoded = WebUtility.HtmlEncode(json);
        var html = JsonToken.Replace(encoded, m =>
        {
            if (m.Groups[1].Success)
                return m.Groups[2].Success
                    ? $"<span class=\"j-k\">{m.Groups[1].Value}</span>{m.Groups[2].Value}"
                    : $"<span class=\"j-s\">{m.Groups[1].Value}</span>";
            if (m.Groups[3].Success) return $"<span class=\"j-b\">{m.Value}</span>";
            return $"<span class=\"j-n\">{m.Value}</span>";
        });
        return new MarkupString(html);
    }

    public static string TypeClass(string? messageType) => messageType switch
    {
        null => "t-state",
        _ when messageType.EndsWith("BIRTH") => "t-birth",
        _ when messageType.EndsWith("DEATH") => "t-death",
        _ when messageType.EndsWith("CMD") => "t-cmd",
        _ => "t-data",
    };

    /// <summary>One-line text for a decoded Sparkplug value.</summary>
    public static string Value(object? v) => v switch
    {
        null => "null",
        bool b => b ? "true" : "false",
        float f => f.ToString("G7", CultureInfo.InvariantCulture),
        double d => d.ToString("G12", CultureInfo.InvariantCulture),
        string s => JsonSerializer.Serialize(s, CompactOptions),
        DateTimeOffset d => d.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
        SpArray a => "[" + string.Join(", ", a.Items.Take(40).Select(Value)) + (a.Items.Count > 40 ? $", … {a.Items.Count} items" : "") + "]",
        SpBytes b => b.Data.Length == 0 ? "0 bytes" : HexShort(b.Data),
        SpDataSet ds => $"DataSet, {ds.Columns.Count} columns × {ds.Rows.Count} rows",
        SpTemplate t => "Template " + (t.TemplateRef ?? ""),
        IDictionary<string, object?> d => "{" + string.Join(", ", d.Select(kv => $"{kv.Key}={Value(kv.Value)}")) + "}",
        IEnumerable e => "[" + string.Join(", ", e.Cast<object?>().Select(Value)) + "]",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => v.ToString() ?? "",
    };
}
