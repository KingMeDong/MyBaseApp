using System;
using System.Collections.Generic;
using System.Linq;
using MyBase.Models.Finance;

namespace MyBase.Services.Trading.Analysis;

/// <summary>
/// Erkennt Marktstruktur (Swing Highs / Swing Lows) basierend auf Williams Fractals.
/// Standard: 3 Kerzen links, 3 Kerzen rechts.
/// </summary>
public class StructureDetector : IPatternDetector {
    private readonly int _leftBars;
    private readonly int _rightBars;

    public StructureDetector(int leftBars = 3, int rightBars = 3) {
        _leftBars = leftBars;
        _rightBars = rightBars;
    }

    public IEnumerable<Signal> Detect(List<Bar1m> bars) {
        var signals = new List<Signal>();
        if (bars == null || bars.Count < _leftBars + _rightBars + 1)
            return signals;

        // Wir brauchen genug Platz links und rechts
        for (int i = _leftBars; i < bars.Count - _rightBars; i++) {
            var current = bars[i];
            
            // 1. Swing High Check
            // High[i] muss > alle Highs links und rechts sein
            bool isSwingHigh = true;
            
            // Links checken
            for (int k = 1; k <= _leftBars; k++) {
                if (bars[i - k].High >= current.High) {
                    isSwingHigh = false;
                    break;
                }
            }
            // Rechts checken (nur wenn links ok war)
            if (isSwingHigh) {
                for (int k = 1; k <= _rightBars; k++) {
                    if (bars[i + k].High >= current.High) { // >= um bei gleichen Highs das erste/letzte zu nehmen? Hier strikt: wenn gleich, kein Fractal
                        isSwingHigh = false;
                        break;
                    }
                }
            }

            if (isSwingHigh) {
                signals.Add(new Signal(
                    TsUtc: current.TsUtc,
                    InstrumentId: current.InstrumentId,
                    Type: SignalType.SwingHigh,
                    Direction: SignalDirection.Short, // Ein Swing High ist oft ein Widerstand -> Short Bias
                    PriceLevel: current.High,
                    PriceTarget: null,
                    StopLoss: null, // Stop wäre knapp drüber
                    DetectorName: nameof(StructureDetector),
                    Description: $"Swing High ({_leftBars}-{_rightBars})"
                ));
            }

            // 2. Swing Low Check
            // Low[i] muss < alle Lows links und rechts sein
            bool isSwingLow = true;

            // Links checken
            for (int k = 1; k <= _leftBars; k++) {
                if (bars[i - k].Low <= current.Low) {
                    isSwingLow = false;
                    break;
                }
            }
            // Rechts checken
            if (isSwingLow) {
                for (int k = 1; k <= _rightBars; k++) {
                    if (bars[i + k].Low <= current.Low) {
                        isSwingLow = false;
                        break;
                    }
                }
            }

            if (isSwingLow) {
                signals.Add(new Signal(
                    TsUtc: current.TsUtc,
                    InstrumentId: current.InstrumentId,
                    Type: SignalType.SwingLow,
                    Direction: SignalDirection.Long, // Ein Swing Low ist oft Support -> Long Bias
                    PriceLevel: current.Low,
                    PriceTarget: null,
                    StopLoss: null, // Stop wäre knapp drunter
                    DetectorName: nameof(StructureDetector),
                    Description: $"Swing Low ({_leftBars}-{_rightBars})"
                ));
            }
        }

        return signals;
    }
}
