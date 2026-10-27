using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Components.Forms;

namespace SparkExplorer.Services;

/// <summary>
/// Saved broker profiles, shared by all browser tabs and kept in a JSON settings file
/// (appsettings.brokers.json next to appsettings.json by default; set BrokerStore:Path to move it,
/// for example onto a container volume). Uploaded certificates are stored in a "certs" folder beside it.
/// </summary>
public sealed class BrokerStore
{
    private const long MaxCertificateBytes = 1024 * 1024;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly object _lock = new();
    private readonly ILogger<BrokerStore> _log;
    private List<ConnectionSettings> _brokers = new();

    public BrokerStore(IConfiguration config, IHostEnvironment env, ILogger<BrokerStore> log)
    {
        _log = log;
        var configured = config["BrokerStore:Path"];
        FilePath = Path.GetFullPath(string.IsNullOrWhiteSpace(configured)
            ? Path.Combine(env.ContentRootPath, "appsettings.brokers.json")
            : Path.Combine(env.ContentRootPath, configured));
        DataDirectory = Path.GetDirectoryName(FilePath)!;
        Load(config);
    }

    public string FilePath { get; }
    public string DataDirectory { get; }
    public string CertificateDirectory => Path.Combine(DataDirectory, "certs");
    public string? LastError { get; private set; }

    /// <summary>Raised after a broker is saved or deleted.</summary>
    public event Action? Changed;

    /// <summary>Copies of all saved brokers, in saved order.</summary>
    public List<ConnectionSettings> All
    {
        get
        {
            lock (_lock) return _brokers.Select(b => b.Clone()).ToList();
        }
    }

    public ConnectionSettings? Get(string? id)
    {
        lock (_lock) return _brokers.FirstOrDefault(b => b.Id == id)?.Clone();
    }

    /// <summary>Adds or replaces a broker and writes the settings file. Throws when the file cannot be written.</summary>
    public void Save(ConnectionSettings broker)
    {
        var copy = WithoutUnsavedSecrets(broker.Clone());
        lock (_lock)
        {
            var i = _brokers.FindIndex(b => b.Id == copy.Id);
            if (i >= 0) _brokers[i] = copy;
            else _brokers.Add(copy);
            Persist();
        }
        Changed?.Invoke();
    }

    public void Delete(string id)
    {
        lock (_lock)
        {
            _brokers.RemoveAll(b => b.Id == id);
            Persist();
        }
        Changed?.Invoke();
    }

    /// <summary>Resolves a certificate path; relative paths are relative to the settings file's folder.</summary>
    public string? ResolvePath(string? path) =>
        string.IsNullOrWhiteSpace(path) ? null
        : Path.IsPathRooted(path.Trim()) ? path.Trim()
        : Path.GetFullPath(Path.Combine(DataDirectory, path.Trim()));

    /// <summary>Stores an uploaded certificate or key file and returns its path relative to the settings folder.</summary>
    public async Task<string> SaveCertificateAsync(IBrowserFile file)
    {
        if (file.Size > MaxCertificateBytes) throw new InvalidOperationException("Certificate files must be smaller than 1 MB.");
        Directory.CreateDirectory(CertificateDirectory);

        var name = string.Concat(Path.GetFileName(file.Name).Select(c => char.IsLetterOrDigit(c) || c is '.' or '-' or '_' ? c : '_'));
        if (string.IsNullOrWhiteSpace(name.Trim('.'))) name = "certificate.pem";
        var target = Path.Combine(CertificateDirectory, name);
        for (var n = 2; File.Exists(target); n++)
            target = Path.Combine(CertificateDirectory, $"{Path.GetFileNameWithoutExtension(name)}-{n}{Path.GetExtension(name)}");

        await using (var input = file.OpenReadStream(MaxCertificateBytes))
        await using (var output = File.Create(target))
        {
            await input.CopyToAsync(output);
        }
        return Path.GetRelativePath(DataDirectory, target).Replace('\\', '/');
    }

    private static ConnectionSettings WithoutUnsavedSecrets(ConnectionSettings b)
    {
        if (!b.SavePasswords)
        {
            b.Password = null;
            b.Tls.ClientCertificatePassword = null;
        }
        return b;
    }

    private void Load(IConfiguration config)
    {
        try
        {
            if (File.Exists(FilePath))
            {
                var file = JsonSerializer.Deserialize<BrokerFile>(File.ReadAllText(FilePath), JsonOptions);
                _brokers = file?.Brokers ?? new();
                Normalize();
                _log.LogInformation("Loaded {Count} brokers from {Path}", _brokers.Count, FilePath);
                return;
            }
        }
        catch (Exception ex)
        {
            LastError = $"Could not read {FilePath}: {ex.Message}";
            _log.LogError(ex, "Could not read broker settings from {Path}", FilePath);
        }

        // First run: seed from appsettings.json ("Brokers" list, or the single "Mqtt" section).
        var seeded = config.GetSection("Brokers").Get<List<ConnectionSettings>>();
        if (seeded is { Count: > 0 })
        {
            _brokers = seeded;
        }
        else
        {
            var single = new ConnectionSettings();
            config.GetSection("Mqtt").Bind(single);
            _brokers = new() { single };
        }
        Normalize();

        // Write the file right away so it can be found and edited by hand.
        try
        {
            if (!File.Exists(FilePath)) Persist();
        }
        catch (Exception ex)
        {
            LastError = $"Could not create {FilePath}: {ex.Message}. Saved brokers will be lost when the app stops.";
            _log.LogWarning(ex, "Could not create broker settings file {Path}", FilePath);
        }
    }

    private void Normalize()
    {
        var ids = new HashSet<string>();
        foreach (var b in _brokers)
        {
            b.Tls ??= new TlsSettings();
            if (string.IsNullOrWhiteSpace(b.Id) || !ids.Add(b.Id))
            {
                b.Id = Guid.NewGuid().ToString("N")[..12];
                ids.Add(b.Id);
            }
            if (string.IsNullOrWhiteSpace(b.Name)) b.Name = b.Describe();
        }
    }

    private void Persist()
    {
        Directory.CreateDirectory(DataDirectory);
        var json = JsonSerializer.Serialize(new BrokerFile { Brokers = _brokers }, JsonOptions);
        var temp = FilePath + ".tmp";
        File.WriteAllText(temp, json);
        File.Move(temp, FilePath, overwrite: true);
        LastError = null;
    }

    private sealed class BrokerFile
    {
        public List<ConnectionSettings> Brokers { get; set; } = new();
    }
}

/// <summary>What the broker dialog returns: the edited profile, and whether to connect or delete it.</summary>
public sealed record BrokerDialogResult(ConnectionSettings Profile, bool Connect, bool Deleted);
