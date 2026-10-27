using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using SparkExplorer.Sparkplug;

namespace SparkExplorer.Services;

/// <summary>Generates simulated plain MQTT and Sparkplug B traffic for trying the UI without a broker.</summary>
public sealed class DemoGenerator
{
    private const string Pump = "spBv1.0/Plant1/DDATA/Edge-01/Pump-1";
    private readonly Random _r = new();
    private ulong _seq;
    private int _tick;

    private static ulong Now => (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private double R => _r.NextDouble();

    public static string FirstTopic => Pump;

    public List<Incoming> Births()
    {
        _seq = 0;
        var now = Now;
        var list = new List<Incoming>
        {
            Plain("spBv1.0/STATE/scada-primary", JsonSerializer.Serialize(new { online = true, timestamp = now }), 1, true),
            Sp("spBv1.0/Plant1/NBIRTH/Edge-01",
                M("bdSeq", null, SpDataTypes.UInt64, PbValue.OfLong(0UL), now),
                M("Node Control/Rebirth", 100, SpDataTypes.Boolean, PbValue.OfBool(false), now),
                M("Properties/Hardware Make", 101, SpDataTypes.String, PbValue.OfString("Raspberry Pi 5"), now),
                M("Uptime", 102, SpDataTypes.Int64, PbValue.OfLong(0L), now)),
        };

        var temp = M("Temperature", 1, SpDataTypes.Float, PbValue.OfFloat(41.2f), now);
        temp.Properties = new PbPropertySet().Add("engUnit", SpDataTypes.String, PbValue.OfString("°C"));
        var batch = new PbDataSet { NumOfColumns = 2 };
        batch.Columns.AddRange(new[] { "Batch", "Qty" });
        batch.Types.AddRange(new[] { SpDataTypes.String, SpDataTypes.Int32 });
        batch.Rows.Add(new List<PbValue?> { PbValue.OfString("B-1041"), PbValue.OfInt(250) });
        batch.Rows.Add(new List<PbValue?> { PbValue.OfString("B-1042"), PbValue.OfInt(180) });

        list.Add(Sp("spBv1.0/Plant1/DBIRTH/Edge-01/Pump-1",
            temp,
            M("Pressure", 2, SpDataTypes.Double, PbValue.OfDouble(3.214), now),
            M("Running", 3, SpDataTypes.Boolean, PbValue.OfBool(true), now),
            M("Speed", 4, SpDataTypes.Int32, PbValue.OfInt(1450), now),
            M("Total Count", 5, SpDataTypes.UInt64, PbValue.OfLong(1203345UL), now),
            M("Status", 6, SpDataTypes.String, PbValue.OfString("RUN"), now),
            M("Vibration Spectrum", 7, SpDataTypes.FloatArray, PbValue.OfBytes(F32(0.12f, 0.4f, 0.33f, 0.08f)), now),
            M("Last Service", 8, SpDataTypes.DateTime, PbValue.OfLong(now - 86_400_000UL * 12), now),
            M("Batch", 9, SpDataTypes.DataSet, PbValue.OfDataSet(batch), now)));

        list.Add(ValveBirth(35, true, -3));
        return list;
    }

    public List<Incoming> Tick()
    {
        var t = ++_tick;
        var now = Now;
        var list = new List<Incoming>();

        var pump = new List<PbMetric>();
        if (R < .8) pump.Add(A(1, PbValue.OfFloat((float)(40 + Math.Sin(t / 6.0) * 4 + R)), now));
        if (R < .6) pump.Add(A(2, PbValue.OfDouble(3.2 + R * .1), now));
        if (R < .4) pump.Add(A(4, PbValue.OfInt(R < .1 ? -150 : 1400 + (int)(R * 100)), now));
        pump.Add(A(5, PbValue.OfLong(1203345UL + (ulong)t * 7), now));
        if (R < .1) pump.Add(A(6, PbValue.OfString(R < .5 ? "RUN" : "WARN"), now));
        if (t % 4 == 0) pump.Add(A(7, PbValue.OfBytes(F32((float)R / 2, (float)R / 2, (float)R / 2, (float)R / 2)), now));
        list.Add(Sp(Pump, pump.ToArray()));

        if (t % 3 == 0)
            list.Add(Sp("spBv1.0/Plant1/DDATA/Edge-01/Valve-2",
                A(20, PbValue.OfInt((int)(30 + R * 20)), now),
                A(22, PbValue.OfInt((int)(R * 10) - 5), now)));
        if (t % 5 == 0) list.Add(Sp("spBv1.0/Plant1/NDATA/Edge-01", A(102, PbValue.OfLong((long)t), now)));
        if (t % 40 == 20) list.Add(Sp("spBv1.0/Plant1/DDEATH/Edge-01/Valve-2"));
        if (t % 40 == 24) list.Add(ValveBirth(0, false, 0));

        list.Add(Plain("factory/line1/temperature", (21 + Math.Sin(t / 8.0) * 2).ToString("F2", System.Globalization.CultureInfo.InvariantCulture)));
        if (t % 2 == 0)
            list.Add(Plain("factory/line1/status", JsonSerializer.Serialize(new
            {
                state = R < .9 ? "running" : "idle",
                oee = Math.Round(0.7 + R * .2, 3),
                shift = "B",
                ts = DateTime.UtcNow.ToString("o"),
            }), 1, true));
        if (t % 3 == 0) list.Add(Plain("factory/line2/counter", (5000 + t).ToString()));
        if (t % 7 == 0) list.Add(Plain("home/livingroom/light", R < .5 ? "ON" : "OFF", 0, true));
        if (t % 5 == 0)
        {
            var blob = new byte[24];
            _r.NextBytes(blob);
            list.Add(new Incoming("sensors/raw/blob", blob, 0, false, DateTime.Now));
        }
        return list;
    }

    private Incoming ValveBirth(int position, bool open, int offset)
    {
        var now = Now;
        return Sp("spBv1.0/Plant1/DBIRTH/Edge-01/Valve-2",
            M("Position", 20, SpDataTypes.Int16, PbValue.OfInt(position), now),
            M("Open", 21, SpDataTypes.Boolean, PbValue.OfBool(open), now),
            M("Offset", 22, SpDataTypes.Int8, PbValue.OfInt(offset), now));
    }

    private Incoming Sp(string topic, params PbMetric[] metrics)
    {
        var p = new PbPayload { Timestamp = Now, Seq = _seq };
        p.Metrics.AddRange(metrics);
        _seq = (_seq + 1) % 256;
        return new Incoming(topic, SparkplugCodec.Encode(p), 0, false, DateTime.Now);
    }

    private static Incoming Plain(string topic, string text, int qos = 0, bool retain = false) =>
        new(topic, Encoding.UTF8.GetBytes(text), qos, retain, DateTime.Now);

    private static PbMetric M(string name, ulong? alias, uint datatype, PbValue value, ulong ts) =>
        new() { Name = name, Alias = alias, Datatype = datatype, Value = value, Timestamp = ts };

    // DATA messages normally carry only the alias; name and datatype come from the birth certificate.
    private static PbMetric A(ulong alias, PbValue value, ulong ts) => new() { Alias = alias, Value = value, Timestamp = ts };

    private static byte[] F32(params float[] values)
    {
        var b = new byte[values.Length * 4];
        for (var i = 0; i < values.Length; i++) BinaryPrimitives.WriteSingleLittleEndian(b.AsSpan(i * 4), values[i]);
        return b;
    }
}
