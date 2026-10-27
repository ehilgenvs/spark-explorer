using System.Text.Json;

namespace SparkExplorer.Services;

/// <summary>Recent numeric history of one data point (a Sparkplug metric, a numeric topic or a JSON field).</summary>
public sealed class DataSeries
{
    public const int MaxPoints = 2000;

    private readonly List<(DateTime Time, double Value)> _points = new();

    public required string Key { get; init; }
    /// <summary>Short name, e.g. the metric name or JSON path.</summary>
    public required string Label { get; init; }
    /// <summary>Where it comes from, e.g. "Plant1/Edge-01/Pump-1" or the topic.</summary>
    public required string Source { get; init; }
    public string? Unit { get; set; }

    public IReadOnlyList<(DateTime Time, double Value)> Points => _points;
    public double? Last => _points.Count > 0 ? _points[^1].Value : null;

    public void Add(DateTime time, double value)
    {
        if (_points.Count == 0 || time >= _points[^1].Time)
        {
            _points.Add((time, value));
        }
        else
        {
            // Historical or late values: keep the list ordered by time.
            var lo = 0;
            var hi = _points.Count;
            while (lo < hi)
            {
                var mid = (lo + hi) / 2;
                if (_points[mid].Time <= time) lo = mid + 1;
                else hi = mid;
            }
            _points.Insert(lo, (time, value));
        }
        if (_points.Count > MaxPoints) _points.RemoveRange(0, _points.Count - MaxPoints);
    }

    /// <summary>Points at or after <paramref name="from"/>, thinned to at most <paramref name="max"/> points.</summary>
    public List<(DateTime Time, double Value)> Window(DateTime? from, int max = 800)
    {
        var start = 0;
        if (from is { } f)
        {
            var lo = 0;
            var hi = _points.Count;
            while (lo < hi)
            {
                var mid = (lo + hi) / 2;
                if (_points[mid].Time < f) lo = mid + 1;
                else hi = mid;
            }
            start = lo;
        }
        var count = _points.Count - start;
        var result = new List<(DateTime, double)>(Math.Min(count, max));
        if (count <= max)
        {
            for (var i = start; i < _points.Count; i++) result.Add(_points[i]);
        }
        else
        {
            var step = (double)count / max;
            for (var k = 0; k < max - 1; k++) result.Add(_points[start + (int)(k * step)]);
            result.Add(_points[^1]);
        }
        return result;
    }
}

public static class SeriesKeys
{
    public static string ForMetric(string spKey, string metricName) => $"sp|{spKey}|{metricName}";
    public static string ForTopic(string topic, string jsonPath) => $"t|{topic}|{jsonPath}";

    /// <summary>Converts a decoded value to a plottable number; booleans become 0/1.</summary>
    public static double? ToDouble(object? v)
    {
        double? d = v switch
        {
            bool b => b ? 1 : 0,
            sbyte x => x,
            byte x => x,
            short x => x,
            ushort x => x,
            int x => x,
            uint x => x,
            long x => x,
            ulong x => x,
            float x => x,
            double x => x,
            decimal x => (double)x,
            _ => null,
        };
        return d is { } n && double.IsFinite(n) ? n : null;
    }
}

/// <summary>Finds numeric (and boolean) values inside a JSON payload.</summary>
public static class JsonPoints
{
    private const int MaxDepth = 5;
    private const int MaxPoints = 64;
    private const int MaxArrayItems = 16;

    /// <summary>Returns (path, value) pairs. The path of a bare number payload is "".</summary>
    public static List<(string Path, double Value)> Extract(JsonElement root)
    {
        var list = new List<(string, double)>();
        Walk(root, "", 0, list);
        return list;
    }

    private static void Walk(JsonElement e, string path, int depth, List<(string, double)> into)
    {
        if (into.Count >= MaxPoints) return;
        switch (e.ValueKind)
        {
            case JsonValueKind.Number:
                if (e.TryGetDouble(out var d) && double.IsFinite(d)) into.Add((path, d));
                break;
            case JsonValueKind.True:
                into.Add((path, 1));
                break;
            case JsonValueKind.False:
                into.Add((path, 0));
                break;
            case JsonValueKind.Object when depth < MaxDepth:
                foreach (var p in e.EnumerateObject())
                    Walk(p.Value, path.Length == 0 ? p.Name : $"{path}.{p.Name}", depth + 1, into);
                break;
            case JsonValueKind.Array when depth < MaxDepth:
                var i = 0;
                foreach (var item in e.EnumerateArray())
                {
                    if (i >= MaxArrayItems) break;
                    Walk(item, $"{path}[{i}]", depth + 1, into);
                    i++;
                }
                break;
        }
    }
}
