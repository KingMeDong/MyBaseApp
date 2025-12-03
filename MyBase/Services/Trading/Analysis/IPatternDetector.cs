using System.Collections.Generic;
using MyBase.Models.Finance;

namespace MyBase.Services.Trading.Analysis;

public interface IPatternDetector {
    /// <summary>
    /// Analysiert eine Liste von Bars und sucht nach Mustern.
    /// </summary>
    /// <param name="bars">Die Historie (chronologisch sortiert). Das letzte Element ist die "aktuelle" Kerze.</param>
    /// <returns>Liste gefundener Signale (meistens 0 oder 1).</returns>
    IEnumerable<Signal> Detect(List<Bar1m> bars);
}
