package restlytics

import "testing"

func iv(start, end int64) Interval { return Interval{Start: start, End: end} }

func TestUnionLength_Empty(t *testing.T) {
	if got := UnionLength(nil); got != 0 {
		t.Fatalf("got %d, want 0", got)
	}
}

func TestUnionLength_Single(t *testing.T) {
	if got := UnionLength([]Interval{iv(0, 10)}); got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
}

func TestUnionLength_DisjointSum(t *testing.T) {
	// [0,10] + [20,25] = 10 + 5
	if got := UnionLength([]Interval{iv(0, 10), iv(20, 25)}); got != 15 {
		t.Fatalf("got %d, want 15", got)
	}
}

func TestUnionLength_OverlappingUnioned(t *testing.T) {
	// [0,10] and [5,15] overlap -> union [0,15] = 15 (NOT 20)
	if got := UnionLength([]Interval{iv(0, 10), iv(5, 15)}); got != 15 {
		t.Fatalf("got %d, want 15", got)
	}
}

func TestUnionLength_FullyContained(t *testing.T) {
	// [2,4] inside [0,10] -> 10
	if got := UnionLength([]Interval{iv(0, 10), iv(2, 4)}); got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
}

func TestUnionLength_AdjacentTouchingMerge(t *testing.T) {
	// [0,10] and [10,20] touch at 10 -> [0,20] = 20
	if got := UnionLength([]Interval{iv(0, 10), iv(10, 20)}); got != 20 {
		t.Fatalf("got %d, want 20", got)
	}
}

func TestUnionLength_UnsortedInput(t *testing.T) {
	if got := UnionLength([]Interval{iv(20, 25), iv(0, 10)}); got != 15 {
		t.Fatalf("got %d, want 15", got)
	}
}

func TestUnionLength_MultipleOverlapsChained(t *testing.T) {
	// [0,5],[3,8],[7,12] all chain -> [0,12] = 12
	if got := UnionLength([]Interval{iv(0, 5), iv(3, 8), iv(7, 12)}); got != 12 {
		t.Fatalf("got %d, want 12", got)
	}
}

func TestUnionLength_ZeroLengthIntervals(t *testing.T) {
	// Zero-length markers contribute nothing on their own.
	if got := UnionLength([]Interval{iv(5, 5), iv(10, 10)}); got != 0 {
		t.Fatalf("got %d, want 0", got)
	}
}

func TestUnionLength_DoesNotMutateInput(t *testing.T) {
	in := []Interval{iv(20, 25), iv(0, 10)}
	_ = UnionLength(in)
	if in[0] != iv(20, 25) || in[1] != iv(0, 10) {
		t.Fatalf("input slice was mutated: %+v", in)
	}
}
