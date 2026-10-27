using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace SparkExplorer.Services;

/// <summary>Loads CA and client certificates from PEM, CRT/DER or PFX/P12 files.</summary>
public static class CertificateFiles
{
    public static X509Certificate2Collection LoadCaCertificates(string path)
    {
        try
        {
            var collection = new X509Certificate2Collection();
            if (IsPem(path)) collection.ImportFromPemFile(path);
            else collection.Add(new X509Certificate2(path));
            if (collection.Count == 0) throw new CryptographicException("The file contains no certificates.");
            return collection;
        }
        catch (Exception ex) when (ex is CryptographicException or IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException($"Could not read the CA certificate \"{path}\": {ex.Message}", ex);
        }
    }

    public static X509Certificate2 LoadClientCertificate(string certificatePath, string? keyPath, string? password)
    {
        try
        {
            var ext = Path.GetExtension(certificatePath).ToLowerInvariant();
            if (ext is ".pfx" or ".p12")
                return new X509Certificate2(certificatePath, password, X509KeyStorageFlags.Exportable);

            var pem = string.IsNullOrEmpty(password)
                ? X509Certificate2.CreateFromPemFile(certificatePath, keyPath)
                : X509Certificate2.CreateFromEncryptedPemFile(certificatePath, password, keyPath);
            if (!pem.HasPrivateKey) throw new CryptographicException("No private key was found. Add the key file, or use a PEM file that contains the key.");

            // TLS on Windows cannot use the ephemeral key of a PEM-loaded certificate; re-importing as PKCS#12 fixes that.
            using (pem)
            {
                return new X509Certificate2(pem.Export(X509ContentType.Pkcs12), (string?)null, X509KeyStorageFlags.Exportable);
            }
        }
        catch (Exception ex) when (ex is CryptographicException or IOException or UnauthorizedAccessException or ArgumentException)
        {
            throw new InvalidOperationException($"Could not read the client certificate \"{certificatePath}\": {ex.Message}", ex);
        }
    }

    private static bool IsPem(string path)
    {
        using var reader = new StreamReader(path);
        var buffer = new char[4096];
        var read = reader.Read(buffer, 0, buffer.Length);
        return new string(buffer, 0, read).Contains("-----BEGIN", StringComparison.Ordinal);
    }
}
