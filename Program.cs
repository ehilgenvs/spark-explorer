using MudBlazor.Services;
using SparkExplorer.Components;
using SparkExplorer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddMudServices();

// Saved broker profiles, shared by all tabs (stored in appsettings.brokers.json).
builder.Services.AddSingleton<BrokerStore>();

// One MQTT session per browser tab (Blazor circuit).
builder.Services.AddScoped<MqttSession>();

var app = builder.Build();

// Load (or create) the saved brokers at startup, so file problems show up in the log right away.
var brokers = app.Services.GetRequiredService<BrokerStore>();
app.Logger.LogInformation("Saved brokers: {Path}", brokers.FilePath);

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}

app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
