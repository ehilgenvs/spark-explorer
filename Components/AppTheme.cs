using MudBlazor;

namespace SparkExplorer.Components;

/// <summary>
/// Muted grey palette in the spirit of ISA-101 HMI screens: colour is reserved for state
/// (online/offline, birth/death, commands) and for the selected data points.
/// </summary>
public static class AppTheme
{
    public static readonly string[] FontSans = { "IBM Plex Sans", "system-ui", "-apple-system", "Segoe UI", "Roboto", "sans-serif" };

    public static readonly MudTheme Theme = new()
    {
        PaletteLight = new PaletteLight
        {
            Primary = "#2f5d8a",
            Secondary = "#6a4c93",
            Info = "#2f5d8a",
            Success = "#2e7d4f",
            Warning = "#9a6200",
            Error = "#b3261e",
            Background = "#dfe2df",
            BackgroundGray = "#e6e9e5",
            Surface = "#eef0ed",
            AppbarBackground = "#2c3236",
            AppbarText = "#eef0ed",
            TextPrimary = "#22272b",
            TextSecondary = "#5f676d",
            ActionDefault = "#5f676d",
            LinesDefault = "#c5cac4",
            LinesInputs = "#a9afa8",
            TableLines = "#d3d7d2",
            TableHover = "#e3e7e2",
            Divider = "#c5cac4",
        },
        PaletteDark = new PaletteDark
        {
            Primary = "#7fa7d1",
            Secondary = "#a991d4",
            Info = "#7fa7d1",
            Success = "#5fbf86",
            Warning = "#e0a63c",
            Error = "#ef7a70",
            Background = "#181b1d",
            BackgroundGray = "#272c30",
            Surface = "#212528",
            AppbarBackground = "#2a3034",
            AppbarText = "#d9dee1",
            TextPrimary = "#d9dee1",
            TextSecondary = "#8d969c",
            ActionDefault = "#a3abb1",
            LinesDefault = "#363c41",
            LinesInputs = "#4a5157",
            TableLines = "#30363b",
            TableHover = "#2a2f33",
            Divider = "#363c41",
        },
        Typography = new Typography
        {
            Default = new DefaultTypography { FontFamily = FontSans },
            H6 = new H6Typography { FontFamily = FontSans, FontSize = "1.05rem", FontWeight = "600" },
            Button = new ButtonTypography { FontFamily = FontSans, TextTransform = "none", FontWeight = "500" },
        },
        LayoutProperties = new LayoutProperties { DefaultBorderRadius = "4px" },
    };

    /// <summary>Line colours for plotted data points; mid-tones that read on both light and dark surfaces.</summary>
    public static readonly string[] ChartPalette =
        { "#4a7fb5", "#d08c1a", "#3f9a63", "#d0453a", "#8a6cc0", "#2a9aa8", "#a08a3c", "#8d969c" };
}
