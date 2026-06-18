package restlytics

import "sort"

// Interval-union (sweep-line) helper used to compute per-category "self time".
//
// Why union and not a plain sum: child spans can overlap (parallel HTTP calls,
// async queries, nested instrumentation). Summing their durations double-counts
// the wall-clock time. The union of intervals gives the real wall-clock time
// actually spent inside that category, which is what the dashboard breakdown and
// the ingestion service's self-time rollups expect.
//
// We work in plain int64 nanoseconds.

// Interval is a [Start, End] pair in nanoseconds.
type Interval struct {
	Start int64
	End   int64
}

// UnionLength returns the total wall-clock length covered by the union of the
// given [start, end] intervals.
func UnionLength(intervals []Interval) int64 {
	if len(intervals) == 0 {
		return 0
	}

	// Copy so we don't mutate the caller's slice, then sort by start so a single
	// forward sweep can merge overlaps.
	sorted := make([]Interval, len(intervals))
	copy(sorted, intervals)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Start < sorted[j].Start
	})

	var total int64
	curStart, curEnd := sorted[0].Start, sorted[0].End

	for i := 1; i < len(sorted); i++ {
		s, e := sorted[i].Start, sorted[i].End
		if s > curEnd {
			// Disjoint: bank the current run and start a new one.
			total += curEnd - curStart
			curStart, curEnd = s, e
		} else if e > curEnd {
			// Overlapping: extend the current run.
			curEnd = e
		}
	}

	total += curEnd - curStart
	return total
}
