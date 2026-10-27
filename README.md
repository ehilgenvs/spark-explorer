# Spark Explorer (Blazor)

A web-based MQTT spark explorer that shows plain MQTT messages, decodes **Sparkplug B** and charts selected data points.
The interface is built with [MudBlazor](https://mudblazor.com) 9.
It is a Blazor Server app (.NET 8), so the broker connection is made by the server.
That is what makes plain **TCP** connections possible alongside **WebSocket**.

## Run

Requires the [.NET 8 SDK](https://dotnet.microsoft.com/download).

```bash
dotnet run
```

Then open http://localhost:5080.

## Connecting

Pick the transport in the top bar:

| Transport | What to enter | Typical ports |
|-----------|---------------|---------------|
| TCP       | Host and port | 1883, or 8883 with TLS |
| WebSocket | Full URL      | `ws://host:9001`, `ws://host:8083/mqtt` (EMQX), `ws://host:8000/mqtt` (HiveMQ), `wss://…` for TLS |

**Options** holds the client ID, username/password, MQTT version (3.1.1 or 5.0),
subscription filters (comma-separated, default `#, $SYS/#`), subscription QoS,
and an option to accept untrusted (self-signed) TLS certificates.

Default values come from the `Mqtt` section of `appsettings.json`.

The connection reconnects automatically every 3 seconds if it drops, and resubscribes after reconnecting.

## Features

- **Topic tree:** a live hierarchy with counts and a preview of each topic's latest value; changed values are highlighted. You can filter it by topic name.
- **Topic view:** the latest payload, shown automatically as JSON, text or a hex dump (you can switch formats manually). Shows QoS, retained flag, size, and the last 100 messages per topic.
- **Sparkplug B decoding:**
  - Covers all message types: NBIRTH, NDATA, NDEATH, NCMD and the device equivalents (DBIRTH, DDATA, …).
  - Aliases from birth certificates are resolved, so DATA messages show metric names and correct signed or unsigned types.
  - Handles every datatype, including arrays, DataSet, Template, DateTime and metric properties.
- **Sparkplug nodes tab:** groups, edge nodes and devices with online/offline state, sequence numbers and current metric values. Also shows STATE messages from host applications. A **Rebirth** button sends an NCMD `Node Control/Rebirth`.
- **Publish tab:** text or hex payloads with QoS and retain. **Clear retained** removes a retained message from the selected topic.
- **Start demo:** simulated plant traffic so you can try the app without a broker.
- **Light and dark theme:** follows your system setting; switch with the icon in the app bar.

## Charts

Every numeric value is recorded as it arrives (the last 2,000 values of each data point), so a chart shows
history as soon as you select a point. Booleans are charted as 0/1.

Select a data point with its chart button, or by clicking its row:

- a metric row in the Topic view of a Sparkplug message,
- a metric row in the Sparkplug nodes tab,
- a chip under **Data points** for a plain numeric topic or a numeric field inside a JSON payload.

The chart docks under the tabs and can show several data points at once. Its toolbar has:

- **Time window:** 1 minute to everything kept.
- **Normalize:** scales each line to its own range (0–100 %) so values with very different ranges,
  like 41 °C and 3.2 bar, can be compared. Tooltips and the table still show real values.
- **Freeze**, **taller chart**, and **close** (removes all data points).

The chart also holds still while the pointer is over it, so tooltips stay readable.
The table under the chart shows last, min, max and average for the selected window.

## Mosquitto quick setup (TCP + WebSocket)

```
listener 1883
listener 9001
protocol websockets
allow_anonymous true
```

## Project layout

```
Sparkplug/ProtoWire.cs          protobuf wire reader/writer
Sparkplug/SparkplugCodec.cs     Sparkplug B payload model, decode + encode
Sparkplug/SparkplugDecoder.cs   datatypes, alias resolution, display values
Services/MqttSession.cs         MQTTnet client (TCP/WS/TLS), message store, Sparkplug state
Services/Models.cs              topic tree, payload detection, formatting
Services/DemoGenerator.cs       simulated traffic
Services/DataSeries.cs          per-data-point history and JSON value extraction
Components/AppTheme.cs          MudBlazor theme and chart colours
Components/Pages/Home.razor     main page
Components/Parts/ChartPanel.razor  live time-series chart
Components/Parts/*.razor        topic details, Sparkplug views, publish panel
```

There is no protobuf code generation step: Sparkplug B is decoded with a small hand-written codec.
NuGet dependencies: MQTTnet 4.3.7 and MudBlazor 9.10.0.

## Notes

- MudBlazor's chart only draws Y gridlines at whole-number steps. The chart panel maps values onto a
  1/2/5 × 10ⁿ step grid internally and translates axis labels and tooltips back, so small ranges
  (for example 3.21–3.29) get readable gridlines.
- The chart's height is set as `calc(100%)` on purpose. With a plain `100%`, MudChart re-creates its
  inner chart right after the first render, and a pending resize callback then reaches the disposed
  chart ("There was an exception invoking 'OnElementSizeChanged'").

- Each browser tab gets its own MQTT connection and message store.
- The web UI has no login. Run it locally or behind your own authentication; don't expose it publicly,
  since anyone who can open it can publish to your broker.
