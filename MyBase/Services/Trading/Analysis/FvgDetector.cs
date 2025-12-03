using System.Collections.Generic;
using System.Linq;
using MyBase.Models.Finance;

namespace MyBase.Services.Trading.Analysis;

public class FvgDetector : IPatternDetector {
    private readonly decimal _minGapTicks; // Mindestgröße in Ticks (optional)

    public FvgDetector(decimal minGapTicks = 1) {
        _minGapTicks = minGapTicks;
    }

    public IEnumerable<Signal> Detect(List<Bar1m> bars) {
        // Wir brauchen mindestens 3 Bars für ein FVG
        if (bars.Count < 3) yield break;

        var c1 = bars[bars.Count - 3]; // Vor-Vorgänger
        // var c2 = bars[bars.Count - 2]; // Die "Imbalance"-Kerze (wird implizit geprüft)
        var c3 = bars[bars.Count - 1]; // Aktuelle Kerze

        // Bullish FVG:
        // C1 High < C3 Low
        // Gap = [C1.High ... C3.Low]
        if (c1.High < c3.Low) {
            var gapSize = c3.Low - c1.High;
            // TODO: TickSize Check einbauen, hier erst mal roh
            if (gapSize > 0) {
                yield return new Signal(
                    TsUtc: c3.TsUtc, // Signal ist fertig mit Abschluss von C3
                    InstrumentId: c3.InstrumentId,
                    Type: SignalType.Fvg,
                    Direction: SignalDirection.Long,
                    PriceLevel: c3.Low, // Entry oft am oberen Rand des Gaps (bei Retest)
                    PriceTarget: null,
                    StopLoss: c1.High,  // Wenn wir unter C1 High fallen, ist Gap gefüllt/invalid
                    DetectorName: nameof(FvgDetector),
                    Description: $"Bullish FVG ({gapSize:F2})"
                );
            }
        }

        // Bearish FVG:
        // C1 Low > C3 High
        // Gap = [C3.High ... C1.Low]
        if (c1.Low > c3.High) {
            var gapSize = c1.Low - c3.High;
            if (gapSize > 0) {
                yield return new Signal(
                    TsUtc: c3.TsUtc,
                    InstrumentId: c3.InstrumentId,
                    Type: SignalType.Fvg,
                    Direction: SignalDirection.Short,
                    PriceLevel: c3.High, // Entry am unteren Rand des Gaps
                    PriceTarget: null,
                    StopLoss: c1.Low,
                    DetectorName: nameof(FvgDetector),
                    Description: $"Bearish FVG ({gapSize:F2})"
                );
            }
        }
    }
}
