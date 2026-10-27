using System.Buffers.Binary;
using System.Text;
using K = SparkExplorer.Sparkplug.PbValueKind;

namespace SparkExplorer.Sparkplug;

public static class SpDataTypes
{
    public const uint Int8 = 1, Int16 = 2, Int32 = 3, Int64 = 4, UInt8 = 5, UInt16 = 6, UInt32 = 7, UInt64 = 8,
        Float = 9, Double = 10, Boolean = 11, String = 12, DateTime = 13, Text = 14, Uuid = 15, DataSet = 16,
        Bytes = 17, File = 18, Template = 19, PropertySet = 20, PropertySetList = 21,
        Int8Array = 22, Int16Array = 23, Int32Array = 24, Int64Array = 25, UInt8Array = 26, UInt16Array = 27,
        UInt32Array = 28, UInt64Array = 29, FloatArray = 30, DoubleArray = 31, BooleanArray = 32,
        StringArray = 33, DateTimeArray = 34;

    private static readonly string[] Names =
    {
        "Unknown", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float", "Double",
        "Boolean", "String", "DateTime", "Text", "UUID", "DataSet", "Bytes", "File", "Template", "PropertySet",
        "PropertySetList", "Int8Array", "Int16Array", "Int32Array", "Int64Array", "UInt8Array", "UInt16Array",
        "UInt32Array", "UInt64Array", "FloatArray", "DoubleArray", "BooleanArray", "StringArray", "DateTimeArray",
    };

    public static string Name(uint? dt) => dt is null ? "?" : dt.Value < Names.Length ? Names[dt.Value] : dt.Value.ToString();
}

/// <summary>A parsed Sparkplug B topic: spBv1.0/group/TYPE/edge[/device].</summary>
public sealed record SpTopic(string Group, string MessageType, string EdgeNode, string? Device)
{
    private static readonly HashSet<string> Types = new() { "NBIRTH", "NDEATH", "NDATA", "NCMD", "DBIRTH", "DDEATH", "DDATA", "DCMD" };

    public string NodeKey => $"{Group}/{EdgeNode}";
    public string Key => Device is null ? NodeKey : $"{NodeKey}/{Device}";
    public bool IsBirth => MessageType.EndsWith("BIRTH", StringComparison.Ordinal);
    public bool IsData => MessageType.EndsWith("DATA", StringComparison.Ordinal);

    public static SpTopic? Parse(string topic)
    {
        var p = topic.Split('/');
        if (p.Length < 4 || p[0] != "spBv1.0" || !Types.Contains(p[2])) return null;
        return new SpTopic(p[1], p[2], p[3], p.Length > 4 ? string.Join('/', p, 4, p.Length - 4) : null);
    }

    /// <summary>Returns the host id for spBv1.0/STATE/{host} (Sparkplug 3) or STATE/{host} (Sparkplug 2).</summary>
    public static string? ParseStateHost(string topic)
    {
        var p = topic.Split('/');
        if (p.Length == 3 && p[0] == "spBv1.0" && p[1] == "STATE") return p[2];
        if (p.Length == 2 && p[0] == "STATE") return p[1];
        return null;
    }
}

// ---------------- decoded (display) model ----------------

public sealed class SpPayload
{
    public string? MessageType { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public ulong? Seq { get; init; }
    public string? Uuid { get; init; }
    public int? BodyLength { get; init; }
    public List<SpMetric> Metrics { get; } = new();
}

public sealed class SpMetric
{
    public string? Name { get; init; }
    public ulong? Alias { get; init; }
    public bool NameFromAlias { get; init; }
    public uint? DataType { get; init; }
    public string TypeName { get; init; } = "?";
    public object? Value { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public bool IsNull { get; init; }
    public bool IsHistorical { get; init; }
    public bool IsTransient { get; init; }
    public Dictionary<string, object?>? Properties { get; init; }
    public string? Description { get; init; }
}

public sealed record SpArray(string ElementType, IReadOnlyList<object?> Items);
public sealed record SpBytes(byte[] Data);
public sealed record SpDataSet(IReadOnlyList<string> Columns, IReadOnlyList<string> Types, IReadOnlyList<IReadOnlyList<object?>> Rows);
public sealed record SpTemplate(string? TemplateRef, bool IsDefinition, string? Version, IReadOnlyList<SpMetric> Metrics, IReadOnlyList<SpMetric> Parameters);

/// <summary>
/// Converts raw Sparkplug payloads into display values. Keeps per-edge-node alias tables learned from
/// NBIRTH/DBIRTH so that DATA messages carrying only aliases show metric names and correct datatypes.
/// </summary>
public sealed class SparkplugDecoder
{
    private sealed class AliasTable
    {
        public readonly Dictionary<ulong, (string Name, uint? Type)> ByAlias = new();
        public readonly Dictionary<string, uint?> TypeByName = new();
    }

    private readonly Dictionary<string, AliasTable> _tables = new();

    public void Reset() => _tables.Clear();

    /// <param name="topic">Parsed topic, or null when decoding a payload outside the Sparkplug namespace.</param>
    /// <param name="track">True for live traffic: births update the alias tables.</param>
    public SpPayload Decode(byte[] bytes, SpTopic? topic, bool track)
    {
        var raw = SparkplugCodec.Decode(bytes);

        AliasTable? table = null;
        if (topic != null)
        {
            if (track && topic.MessageType == "NBIRTH") _tables[topic.NodeKey] = new AliasTable();
            if (!_tables.TryGetValue(topic.NodeKey, out table) && track)
            {
                table = new AliasTable();
                _tables[topic.NodeKey] = table;
            }
        }
        var learn = track && topic?.IsBirth == true && table != null;

        var result = new SpPayload
        {
            MessageType = topic?.MessageType,
            Timestamp = raw.Timestamp is { } ts ? FromMillis(ts) : null,
            Seq = raw.Seq,
            Uuid = raw.Uuid,
            BodyLength = raw.Body?.Length,
        };

        foreach (var m in raw.Metrics)
        {
            var name = m.Name;
            var dt = m.Datatype;
            var fromAlias = false;

            if (learn)
            {
                if (name != null && m.Alias is { } a) table!.ByAlias[a] = (name, dt);
                if (name != null) table!.TypeByName[name] = dt;
            }
            if (table != null)
            {
                if (name == null && m.Alias is { } a && table.ByAlias.TryGetValue(a, out var known))
                {
                    name = known.Name;
                    dt ??= known.Type;
                    fromAlias = true;
                }
                else if (name != null && dt == null && table.TypeByName.TryGetValue(name, out var t))
                {
                    dt = t;
                }
            }
            result.Metrics.Add(ConvertMetric(m, name, dt, fromAlias));
        }
        return result;
    }

    private static SpMetric ConvertMetric(PbMetric m, string? name, uint? dt, bool fromAlias) => new()
    {
        Name = name,
        Alias = m.Alias,
        NameFromAlias = fromAlias,
        DataType = dt,
        TypeName = SpDataTypes.Name(dt),
        IsNull = m.IsNull,
        IsHistorical = m.IsHistorical,
        IsTransient = m.IsTransient,
        Timestamp = m.Timestamp is { } ts ? FromMillis(ts) : null,
        Value = m.IsNull ? null : Convert(m.Value, dt ?? 0),
        Properties = m.Properties != null ? ConvertProps(m.Properties) : null,
        Description = m.MetaData?.Description,
    };

    public static object? Convert(PbValue? v, uint dt)
    {
        if (v == null) return null;
        return v.Kind switch
        {
            K.Int => FromInt(v.Int, dt),
            K.Long => FromLong(v.Long, dt),
            K.Float => v.Float,
            K.Double => v.Double,
            K.Bool => v.Bool,
            K.String => v.String,
            K.Bytes => FromBytes(v.Bytes ?? Array.Empty<byte>(), dt),
            K.DataSet => ConvertDataSet(v.DataSet!),
            K.Template => ConvertTemplate(v.Template!),
            K.PropertySet => ConvertProps(v.PropertySet!),
            K.PropertySetList => v.PropertySetList!.Select(ConvertProps).ToList(),
            K.Extension => "(extension value)",
            _ => null,
        };
    }

    // Int8/Int16/Int32 are carried in uint32 as two's complement.
    private static object FromInt(uint v, uint dt) => dt switch
    {
        SpDataTypes.Int8 => unchecked((sbyte)v),
        SpDataTypes.Int16 => unchecked((short)v),
        SpDataTypes.Int32 or SpDataTypes.Int64 => unchecked((int)v),
        SpDataTypes.UInt8 => (byte)v,
        SpDataTypes.UInt16 => (ushort)v,
        _ => (object)v,
    };

    private static object FromLong(ulong v, uint dt) => dt switch
    {
        SpDataTypes.Int8 or SpDataTypes.Int16 or SpDataTypes.Int32 or SpDataTypes.Int64 => unchecked((long)v),
        SpDataTypes.DateTime => FromMillis(v) is { } d ? d : v,
        _ => (object)v,
    };

    private static DateTimeOffset? FromMillis(ulong ms) =>
        ms <= 253402300799999UL ? DateTimeOffset.FromUnixTimeMilliseconds((long)ms) : null;

    private static object FromBytes(byte[] b, uint dt)
    {
        var name = SpDataTypes.Name(dt).Replace("Array", "");
        List<object?> Read(int size, Func<int, object?> read)
        {
            var list = new List<object?>(b.Length / size);
            for (var i = 0; i + size <= b.Length; i += size) list.Add(read(i));
            return list;
        }

        switch (dt)
        {
            case SpDataTypes.Int8Array: return new SpArray(name, Read(1, i => (sbyte)b[i]));
            case SpDataTypes.Int16Array: return new SpArray(name, Read(2, i => BinaryPrimitives.ReadInt16LittleEndian(b.AsSpan(i))));
            case SpDataTypes.Int32Array: return new SpArray(name, Read(4, i => BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(i))));
            case SpDataTypes.Int64Array: return new SpArray(name, Read(8, i => BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(i))));
            case SpDataTypes.UInt8Array: return new SpArray(name, Read(1, i => b[i]));
            case SpDataTypes.UInt16Array: return new SpArray(name, Read(2, i => BinaryPrimitives.ReadUInt16LittleEndian(b.AsSpan(i))));
            case SpDataTypes.UInt32Array: return new SpArray(name, Read(4, i => BinaryPrimitives.ReadUInt32LittleEndian(b.AsSpan(i))));
            case SpDataTypes.UInt64Array: return new SpArray(name, Read(8, i => BinaryPrimitives.ReadUInt64LittleEndian(b.AsSpan(i))));
            case SpDataTypes.FloatArray: return new SpArray(name, Read(4, i => BinaryPrimitives.ReadSingleLittleEndian(b.AsSpan(i))));
            case SpDataTypes.DoubleArray: return new SpArray(name, Read(8, i => BinaryPrimitives.ReadDoubleLittleEndian(b.AsSpan(i))));
            case SpDataTypes.DateTimeArray:
                return new SpArray(name, Read(8, i => (object?)FromMillis((ulong)BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(i)))));
            case SpDataTypes.BooleanArray:
            {
                // 4-byte little-endian element count, then bits packed MSB first.
                var list = new List<object?>();
                if (b.Length >= 4)
                {
                    var count = BinaryPrimitives.ReadUInt32LittleEndian(b);
                    for (var i = 0; i < count && 4 + (i >> 3) < b.Length; i++)
                        list.Add(((b[4 + (i >> 3)] >> (7 - (i & 7))) & 1) == 1);
                }
                return new SpArray(name, list);
            }
            case SpDataTypes.StringArray:
            {
                var s = Encoding.UTF8.GetString(b).TrimEnd('\0');
                return new SpArray(name, s.Length == 0 ? new List<object?>() : s.Split('\0').Cast<object?>().ToList());
            }
            default:
                return new SpBytes(b);
        }
    }

    private static SpDataSet ConvertDataSet(PbDataSet ds)
    {
        var rows = ds.Rows
            .Select(r => (IReadOnlyList<object?>)r.Select((cell, i) => Convert(cell, i < ds.Types.Count ? ds.Types[i] : 0)).ToList())
            .ToList();
        return new SpDataSet(ds.Columns, ds.Types.Select(t => SpDataTypes.Name(t)).ToList(), rows);
    }

    private static SpTemplate ConvertTemplate(PbTemplate t)
    {
        var metrics = t.Metrics.Select(m => ConvertMetric(m, m.Name, m.Datatype, false)).ToList();
        var parameters = t.Parameters.Select(p => new SpMetric
        {
            Name = p.Name,
            DataType = p.Type,
            TypeName = SpDataTypes.Name(p.Type),
            Value = Convert(p.Value, p.Type ?? 0),
        }).ToList();
        return new SpTemplate(t.TemplateRef, t.IsDefinition == true, t.Version, metrics, parameters);
    }

    private static Dictionary<string, object?> ConvertProps(PbPropertySet ps)
    {
        var d = new Dictionary<string, object?>();
        for (var i = 0; i < ps.Keys.Count; i++)
        {
            var pv = i < ps.Values.Count ? ps.Values[i] : null;
            d[ps.Keys[i]] = pv == null || pv.IsNull ? null : Convert(pv.Value, pv.Type ?? 0);
        }
        return d;
    }
}
