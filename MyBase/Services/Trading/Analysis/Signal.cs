using System;
using MyBase.Models.Finance;

namespace MyBase.Services.Trading.Analysis;

public enum SignalDirection {
    Long = 1,
    Short = -1,
    Neutral = 0
}

public enum SignalType {
    Fvg,            // Fair Value Gap
    OrderBlock,
    LiquiditySweep,
    StructureShift,
    SwingHigh,      // Williams Fractal High
    SwingLow        // Williams Fractal Low
}

public record Signal(
    DateTime TsUtc,
    int InstrumentId,
    SignalType Type,
    SignalDirection Direction,
    decimal PriceLevel,     // Der relevante Preis (z.B. FVG Start)
    decimal? PriceTarget,   // Optional: Wo wollen wir hin?
    decimal? StopLoss,      // Optional: Wo ist das Setup invalid?
    string DetectorName,    // Wer hat es gefunden?
    string Description      // Menschelesbare Info
);
