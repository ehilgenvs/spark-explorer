using K = SparkExplorer.Sparkplug.PbValueKind;

namespace SparkExplorer.Sparkplug;

// Raw Sparkplug B payload model, mirroring org.eclipse.tahu.protobuf.Payload (sparkplug_b.proto).

public enum PbValueKind { Int, Long, Float, Double, Bool, String, Bytes, DataSet, Template, PropertySet, PropertySetList, Extension }

public sealed class PbValue
{
    public K Kind { get; init; }
    public uint Int { get; init; }
    public ulong Long { get; init; }
    public float Float { get; init; }
    public double Double { get; init; }
    public bool Bool { get; init; }
    public string? String { get; init; }
    public byte[]? Bytes { get; init; }
    public PbDataSet? DataSet { get; init; }
    public PbTemplate? Template { get; init; }
    public PbPropertySet? PropertySet { get; init; }
    public List<PbPropertySet>? PropertySetList { get; init; }

    public static PbValue OfInt(uint v) => new() { Kind = K.Int, Int = v };
    public static PbValue OfInt(int v) => new() { Kind = K.Int, Int = unchecked((uint)v) };
    public static PbValue OfLong(ulong v) => new() { Kind = K.Long, Long = v };
    public static PbValue OfLong(long v) => new() { Kind = K.Long, Long = unchecked((ulong)v) };
    public static PbValue OfFloat(float v) => new() { Kind = K.Float, Float = v };
    public static PbValue OfDouble(double v) => new() { Kind = K.Double, Double = v };
    public static PbValue OfBool(bool v) => new() { Kind = K.Bool, Bool = v };
    public static PbValue OfString(string v) => new() { Kind = K.String, String = v };
    public static PbValue OfBytes(byte[] v) => new() { Kind = K.Bytes, Bytes = v };
    public static PbValue OfDataSet(PbDataSet v) => new() { Kind = K.DataSet, DataSet = v };
    public static PbValue OfTemplate(PbTemplate v) => new() { Kind = K.Template, Template = v };
}

public sealed class PbPayload
{
    public ulong? Timestamp { get; set; }
    public List<PbMetric> Metrics { get; } = new();
    public ulong? Seq { get; set; }
    public string? Uuid { get; set; }
    public byte[]? Body { get; set; }
}

public sealed class PbMetric
{
    public string? Name { get; set; }
    public ulong? Alias { get; set; }
    public ulong? Timestamp { get; set; }
    public uint? Datatype { get; set; }
    public bool IsHistorical { get; set; }
    public bool IsTransient { get; set; }
    public bool IsNull { get; set; }
    public PbMetaData? MetaData { get; set; }
    public PbPropertySet? Properties { get; set; }
    public PbValue? Value { get; set; }
}

public sealed class PbMetaData
{
    public bool? IsMultiPart { get; set; }
    public string? ContentType { get; set; }
    public ulong? Size { get; set; }
    public ulong? Seq { get; set; }
    public string? FileName { get; set; }
    public string? FileType { get; set; }
    public string? Md5 { get; set; }
    public string? Description { get; set; }
}

public sealed class PbDataSet
{
    public ulong? NumOfColumns { get; set; }
    public List<string> Columns { get; } = new();
    public List<uint> Types { get; } = new();
    public List<List<PbValue?>> Rows { get; } = new();
}

public sealed class PbTemplate
{
    public string? Version { get; set; }
    public List<PbMetric> Metrics { get; } = new();
    public List<PbParameter> Parameters { get; } = new();
    public string? TemplateRef { get; set; }
    public bool? IsDefinition { get; set; }
}

public sealed class PbParameter
{
    public string? Name { get; set; }
    public uint? Type { get; set; }
    public PbValue? Value { get; set; }
}

public sealed class PbPropertySet
{
    public List<string> Keys { get; } = new();
    public List<PbPropertyValue> Values { get; } = new();

    public PbPropertySet Add(string key, uint type, PbValue value)
    {
        Keys.Add(key);
        Values.Add(new PbPropertyValue { Type = type, Value = value });
        return this;
    }
}

public sealed class PbPropertyValue
{
    public uint? Type { get; set; }
    public bool IsNull { get; set; }
    public PbValue? Value { get; set; }
}

/// <summary>Decodes and encodes Sparkplug B protobuf payloads.</summary>
public static class SparkplugCodec
{
    // Field numbers of each oneof "value", in order, starting at the first value field.
    private static readonly K[] MetricKinds = { K.Int, K.Long, K.Float, K.Double, K.Bool, K.String, K.Bytes, K.DataSet, K.Template, K.Extension }; // 10..19
    private static readonly K[] DataSetValueKinds = { K.Int, K.Long, K.Float, K.Double, K.Bool, K.String, K.Extension };                     // 1..7
    private static readonly K[] ParameterKinds = { K.Int, K.Long, K.Float, K.Double, K.Bool, K.String, K.Extension };                        // 3..9
    private static readonly K[] PropertyKinds = { K.Int, K.Long, K.Float, K.Double, K.Bool, K.String, K.PropertySet, K.PropertySetList, K.Extension }; // 3..11

    // ---------------- decoding ----------------

    public static PbPayload Decode(byte[] data)
    {
        var r = new ProtoReader(data);
        var p = new PbPayload();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 0: p.Timestamp = r.ReadVarint(); break;
                case 2 when wt == 2: p.Metrics.Add(ReadMetric(r.ReadMessage())); break;
                case 3 when wt == 0: p.Seq = r.ReadVarint(); break;
                case 4 when wt == 2: p.Uuid = r.ReadString(); break;
                case 5 when wt == 2: p.Body = r.ReadBytes(); break;
                default: r.Skip(wt); break;
            }
        }
        return p;
    }

    private static PbMetric ReadMetric(ProtoReader r)
    {
        var m = new PbMetric();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 2: m.Name = r.ReadString(); break;
                case 2 when wt == 0: m.Alias = r.ReadVarint(); break;
                case 3 when wt == 0: m.Timestamp = r.ReadVarint(); break;
                case 4 when wt == 0: m.Datatype = (uint)r.ReadVarint(); break;
                case 5 when wt == 0: m.IsHistorical = r.ReadVarint() != 0; break;
                case 6 when wt == 0: m.IsTransient = r.ReadVarint() != 0; break;
                case 7 when wt == 0: m.IsNull = r.ReadVarint() != 0; break;
                case 8 when wt == 2: m.MetaData = ReadMetaData(r.ReadMessage()); break;
                case 9 when wt == 2: m.Properties = ReadPropertySet(r.ReadMessage()); break;
                case >= 10 and <= 19: m.Value = ReadValue(r, wt, MetricKinds[f - 10]) ?? m.Value; break;
                default: r.Skip(wt); break;
            }
        }
        return m;
    }

    private static PbValue? ReadValue(ProtoReader r, int wt, K kind)
    {
        switch (kind)
        {
            case K.Int when wt == 0: return PbValue.OfInt((uint)r.ReadVarint());
            case K.Long when wt == 0: return PbValue.OfLong(r.ReadVarint());
            case K.Float when wt == 5: return PbValue.OfFloat(r.ReadFloat());
            case K.Double when wt == 1: return PbValue.OfDouble(r.ReadDouble());
            case K.Bool when wt == 0: return PbValue.OfBool(r.ReadVarint() != 0);
            case K.String when wt == 2: return PbValue.OfString(r.ReadString());
            case K.Bytes when wt == 2: return PbValue.OfBytes(r.ReadBytes());
            case K.DataSet when wt == 2: return PbValue.OfDataSet(ReadDataSet(r.ReadMessage()));
            case K.Template when wt == 2: return PbValue.OfTemplate(ReadTemplate(r.ReadMessage()));
            case K.PropertySet when wt == 2: return new PbValue { Kind = K.PropertySet, PropertySet = ReadPropertySet(r.ReadMessage()) };
            case K.PropertySetList when wt == 2:
            {
                var list = new List<PbPropertySet>();
                var sub = r.ReadMessage();
                while (sub.NextField(out var f, out var w))
                {
                    if (f == 1 && w == 2) list.Add(ReadPropertySet(sub.ReadMessage()));
                    else sub.Skip(w);
                }
                return new PbValue { Kind = K.PropertySetList, PropertySetList = list };
            }
            case K.Extension:
                r.Skip(wt);
                return new PbValue { Kind = K.Extension };
            default:
                r.Skip(wt);
                return null;
        }
    }

    private static PbDataSet ReadDataSet(ProtoReader r)
    {
        var d = new PbDataSet();
        var types = new List<ulong>();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 0: d.NumOfColumns = r.ReadVarint(); break;
                case 2 when wt == 2: d.Columns.Add(r.ReadString()); break;
                case 3: r.ReadVarints(wt, types); break;
                case 4 when wt == 2:
                {
                    var row = new List<PbValue?>();
                    var rr = r.ReadMessage();
                    while (rr.NextField(out var rf, out var rw))
                    {
                        if (rf == 1 && rw == 2) row.Add(ReadOneOf(rr.ReadMessage(), 1, DataSetValueKinds));
                        else rr.Skip(rw);
                    }
                    d.Rows.Add(row);
                    break;
                }
                default: r.Skip(wt); break;
            }
        }
        d.Types.AddRange(types.Select(t => (uint)t));
        return d;
    }

    /// <summary>Reads a message that contains only a oneof value (DataSetValue).</summary>
    private static PbValue? ReadOneOf(ProtoReader r, int firstField, K[] kinds)
    {
        PbValue? v = null;
        while (r.NextField(out var f, out var wt))
        {
            var i = f - firstField;
            if (i >= 0 && i < kinds.Length) v = ReadValue(r, wt, kinds[i]) ?? v;
            else r.Skip(wt);
        }
        return v;
    }

    private static PbTemplate ReadTemplate(ProtoReader r)
    {
        var t = new PbTemplate();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 2: t.Version = r.ReadString(); break;
                case 2 when wt == 2: t.Metrics.Add(ReadMetric(r.ReadMessage())); break;
                case 3 when wt == 2: t.Parameters.Add(ReadParameter(r.ReadMessage())); break;
                case 4 when wt == 2: t.TemplateRef = r.ReadString(); break;
                case 5 when wt == 0: t.IsDefinition = r.ReadVarint() != 0; break;
                default: r.Skip(wt); break;
            }
        }
        return t;
    }

    private static PbParameter ReadParameter(ProtoReader r)
    {
        var p = new PbParameter();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 2: p.Name = r.ReadString(); break;
                case 2 when wt == 0: p.Type = (uint)r.ReadVarint(); break;
                case >= 3 and <= 9: p.Value = ReadValue(r, wt, ParameterKinds[f - 3]) ?? p.Value; break;
                default: r.Skip(wt); break;
            }
        }
        return p;
    }

    private static PbPropertySet ReadPropertySet(ProtoReader r)
    {
        var ps = new PbPropertySet();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 2: ps.Keys.Add(r.ReadString()); break;
                case 2 when wt == 2: ps.Values.Add(ReadPropertyValue(r.ReadMessage())); break;
                default: r.Skip(wt); break;
            }
        }
        return ps;
    }

    private static PbPropertyValue ReadPropertyValue(ProtoReader r)
    {
        var pv = new PbPropertyValue();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 0: pv.Type = (uint)r.ReadVarint(); break;
                case 2 when wt == 0: pv.IsNull = r.ReadVarint() != 0; break;
                case >= 3 and <= 11: pv.Value = ReadValue(r, wt, PropertyKinds[f - 3]) ?? pv.Value; break;
                default: r.Skip(wt); break;
            }
        }
        return pv;
    }

    private static PbMetaData ReadMetaData(ProtoReader r)
    {
        var md = new PbMetaData();
        while (r.NextField(out var f, out var wt))
        {
            switch (f)
            {
                case 1 when wt == 0: md.IsMultiPart = r.ReadVarint() != 0; break;
                case 2 when wt == 2: md.ContentType = r.ReadString(); break;
                case 3 when wt == 0: md.Size = r.ReadVarint(); break;
                case 4 when wt == 0: md.Seq = r.ReadVarint(); break;
                case 5 when wt == 2: md.FileName = r.ReadString(); break;
                case 6 when wt == 2: md.FileType = r.ReadString(); break;
                case 7 when wt == 2: md.Md5 = r.ReadString(); break;
                case 8 when wt == 2: md.Description = r.ReadString(); break;
                default: r.Skip(wt); break;
            }
        }
        return md;
    }

    // ---------------- encoding ----------------

    public static byte[] Encode(PbPayload p)
    {
        var w = new ProtoWriter();
        if (p.Timestamp is { } ts) w.WriteVarint(1, ts);
        foreach (var m in p.Metrics) w.WriteMessage(2, mw => WriteMetric(mw, m));
        if (p.Seq is { } seq) w.WriteVarint(3, seq);
        if (p.Uuid != null) w.WriteString(4, p.Uuid);
        if (p.Body != null) w.WriteBytes(5, p.Body);
        return w.ToArray();
    }

    private static void WriteMetric(ProtoWriter w, PbMetric m)
    {
        if (m.Name != null) w.WriteString(1, m.Name);
        if (m.Alias is { } alias) w.WriteVarint(2, alias);
        if (m.Timestamp is { } ts) w.WriteVarint(3, ts);
        if (m.Datatype is { } dt) w.WriteVarint(4, dt);
        if (m.IsHistorical) w.WriteBool(5, true);
        if (m.IsTransient) w.WriteBool(6, true);
        if (m.IsNull) w.WriteBool(7, true);
        if (m.Properties != null) w.WriteMessage(9, pw => WritePropertySet(pw, m.Properties));
        if (m.Value != null && !m.IsNull) WriteValue(w, m.Value, 10, MetricKinds);
    }

    private static void WriteValue(ProtoWriter w, PbValue v, int firstField, K[] kinds)
    {
        var i = Array.IndexOf(kinds, v.Kind);
        if (i < 0) return;
        var f = firstField + i;
        switch (v.Kind)
        {
            case K.Int: w.WriteVarint(f, v.Int); break;
            case K.Long: w.WriteVarint(f, v.Long); break;
            case K.Float: w.WriteFloat(f, v.Float); break;
            case K.Double: w.WriteDouble(f, v.Double); break;
            case K.Bool: w.WriteBool(f, v.Bool); break;
            case K.String: w.WriteString(f, v.String ?? ""); break;
            case K.Bytes: w.WriteBytes(f, v.Bytes ?? Array.Empty<byte>()); break;
            case K.DataSet: w.WriteMessage(f, dw => WriteDataSet(dw, v.DataSet!)); break;
            case K.Template: w.WriteMessage(f, tw => WriteTemplate(tw, v.Template!)); break;
            case K.PropertySet: w.WriteMessage(f, pw => WritePropertySet(pw, v.PropertySet!)); break;
            case K.PropertySetList:
                w.WriteMessage(f, lw => { foreach (var ps in v.PropertySetList!) lw.WriteMessage(1, pw => WritePropertySet(pw, ps)); });
                break;
            case K.Extension: w.WriteMessage(f, _ => { }); break;
        }
    }

    private static void WriteDataSet(ProtoWriter w, PbDataSet d)
    {
        w.WriteVarint(1, d.NumOfColumns ?? (ulong)d.Columns.Count);
        foreach (var c in d.Columns) w.WriteString(2, c);
        foreach (var t in d.Types) w.WriteVarint(3, t);
        foreach (var row in d.Rows)
        {
            w.WriteMessage(4, rw =>
            {
                foreach (var cell in row)
                    rw.WriteMessage(1, cw => { if (cell != null) WriteValue(cw, cell, 1, DataSetValueKinds); });
            });
        }
    }

    private static void WriteTemplate(ProtoWriter w, PbTemplate t)
    {
        if (t.Version != null) w.WriteString(1, t.Version);
        foreach (var m in t.Metrics) w.WriteMessage(2, mw => WriteMetric(mw, m));
        foreach (var p in t.Parameters)
        {
            w.WriteMessage(3, pw =>
            {
                if (p.Name != null) pw.WriteString(1, p.Name);
                if (p.Type is { } type) pw.WriteVarint(2, type);
                if (p.Value != null) WriteValue(pw, p.Value, 3, ParameterKinds);
            });
        }
        if (t.TemplateRef != null) w.WriteString(4, t.TemplateRef);
        if (t.IsDefinition is { } def) w.WriteBool(5, def);
    }

    private static void WritePropertySet(ProtoWriter w, PbPropertySet ps)
    {
        foreach (var k in ps.Keys) w.WriteString(1, k);
        foreach (var v in ps.Values)
        {
            w.WriteMessage(2, vw =>
            {
                if (v.Type is { } type) vw.WriteVarint(1, type);
                if (v.IsNull) vw.WriteBool(2, true);
                if (v.Value != null && !v.IsNull) WriteValue(vw, v.Value, 3, PropertyKinds);
            });
        }
    }
}
