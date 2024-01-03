﻿using Fugu.Utils;

namespace Fugu.Channels;

/// <summary>
/// Emitted by <see cref="Actors.IndexActor"/> when the usage stats for segments have changed as
/// a result of an index update.
/// </summary>
/// <param name="Clock">Logical clock value.</param>
/// <param name="Stats">Usage stats for all segments part of the index.</param>
public readonly record struct SegmentStatsUpdated(
    VectorClock Clock,
    IReadOnlyDictionary<Segment, SegmentStats> Stats
);
