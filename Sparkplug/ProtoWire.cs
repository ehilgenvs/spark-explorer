using System.Buffers.Binary;
using System.Text;

namespace SparkExplorer.Sparkplug;

/// <summary>Minimal protobuf wire-format reader, enough for the Sparkplug B schema.</summary>
public sealed class ProtoReader
{
    private readonly byte[] _buf;
    private readonly int _end;
    private int _pos;

    public ProtoReader(byte[] buf) : this(buf, 0, buf.Length) { }

    public ProtoReader(byte[] buf, int start, int end)
    {
        _buf = buf;
        _pos = start;
        _end = end;
    }

    public bool End => _pos >= _end;

    public bool NextField(out int field, out int wireType)
    {
        if (_pos >= _end)
        {
            field = 0;
            wireType = 0;
            return false;
        }
        var tag = ReadVarint();
        field = (int)(tag >> 3);
        wireType = (int)(tag & 7);
        if (field <= 0) throw new InvalidDataException("Invalid protobuf field number 0.");
        return true;
    }

    public ulong ReadVarint()
    {
        ulong result = 0;
        var shift = 0;
        while (true)
        {
            if (_pos >= _end) throw new InvalidDataException("Payload ends inside a varint.");
            var b = _buf[_pos++];
            if (shift < 64) result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 63) throw new InvalidDataException("Varint is longer than 10 bytes.");
        }
    }

    public uint ReadFixed32()
    {
        Need(4);
        var v = BinaryPrimitives.ReadUInt32LittleEndian(_buf.AsSpan(_pos, 4));
        _pos += 4;
        return v;
    }

    public ulong ReadFixed64()
    {
        Need(8);
        var v = BinaryPrimitives.ReadUInt64LittleEndian(_buf.AsSpan(_pos, 8));
        _pos += 8;
        return v;
    }

    public float ReadFloat() => BitConverter.UInt32BitsToSingle(ReadFixed32());
    public double ReadDouble() => BitConverter.UInt64BitsToDouble(ReadFixed64());

    public byte[] ReadBytes()
    {
        var len = ReadLength();
        var result = _buf.AsSpan(_pos, len).ToArray();
        _pos += len;
        return result;
    }

    public string ReadString()
    {
        var len = ReadLength();
        var s = Encoding.UTF8.GetString(_buf, _pos, len);
        _pos += len;
        return s;
    }

    public ProtoReader ReadMessage()
    {
        var len = ReadLength();
        var sub = new ProtoReader(_buf, _pos, _pos + len);
        _pos += len;
        return sub;
    }

    /// <summary>Reads a repeated varint field in either packed or unpacked form.</summary>
    public void ReadVarints(int wireType, List<ulong> into)
    {
        if (wireType == 2)
        {
            var sub = ReadMessage();
            while (!sub.End) into.Add(sub.ReadVarint());
        }
        else if (wireType == 0) into.Add(ReadVarint());
        else Skip(wireType);
    }

    public void Skip(int wireType)
    {
        switch (wireType)
        {
            case 0: ReadVarint(); break;
            case 1: Need(8); _pos += 8; break;
            case 2: _pos += ReadLength(); break;
            case 5: Need(4); _pos += 4; break;
            default: throw new InvalidDataException($"Unsupported protobuf wire type {wireType}.");
        }
    }

    private int ReadLength()
    {
        var len = ReadVarint();
        if (len > (ulong)(_end - _pos)) throw new InvalidDataException("A length-delimited field runs past the end of the payload.");
        return (int)len;
    }

    private void Need(int n)
    {
        if (_end - _pos < n) throw new InvalidDataException("Payload is truncated.");
    }
}

/// <summary>Minimal protobuf wire-format writer.</summary>
public sealed class ProtoWriter
{
    private readonly MemoryStream _ms = new();

    public byte[] ToArray() => _ms.ToArray();

    public void WriteVarint(int field, ulong value)
    {
        Tag(field, 0);
        Varint(value);
    }

    public void WriteBool(int field, bool value) => WriteVarint(field, value ? 1UL : 0UL);

    public void WriteFloat(int field, float value)
    {
        Tag(field, 5);
        Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteSingleLittleEndian(b, value);
        _ms.Write(b);
    }

    public void WriteDouble(int field, double value)
    {
        Tag(field, 1);
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(b, value);
        _ms.Write(b);
    }

    public void WriteBytes(int field, ReadOnlySpan<byte> value)
    {
        Tag(field, 2);
        Varint((ulong)value.Length);
        _ms.Write(value);
    }

    public void WriteString(int field, string value) => WriteBytes(field, Encoding.UTF8.GetBytes(value));

    public void WriteMessage(int field, Action<ProtoWriter> body)
    {
        var inner = new ProtoWriter();
        body(inner);
        WriteBytes(field, inner.ToArray());
    }

    private void Tag(int field, int wireType) => Varint(((ulong)(uint)field << 3) | (uint)wireType);

    private void Varint(ulong v)
    {
        while (v >= 0x80)
        {
            _ms.WriteByte((byte)(v | 0x80));
            v >>= 7;
        }
        _ms.WriteByte((byte)v);
    }
}
