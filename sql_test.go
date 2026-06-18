package restlytics

import (
	"strings"
	"testing"
)

func TestNormalize_StripsNumericLiterals(t *testing.T) {
	got := Normalize("SELECT * FROM users WHERE id = 1")
	want := "select * from users where id = ?"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalize_StripsStringLiterals(t *testing.T) {
	got := Normalize("SELECT * FROM users WHERE email = 'alice@example.com'")
	want := "select * from users where email = ?"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalize_DifferentLiteralsSameTemplate(t *testing.T) {
	a := Normalize("SELECT * FROM users WHERE id = 1")
	b := Normalize("SELECT * FROM users WHERE id = 2")
	if a != b {
		t.Fatalf("expected same template, got %q vs %q", a, b)
	}
}

func TestNormalize_CollapsesInLists(t *testing.T) {
	got := Normalize("SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)")
	want := "select * from users where id in (?)"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	short := Normalize("SELECT * FROM users WHERE id IN (1, 2)")
	long := Normalize("SELECT * FROM users WHERE id IN (1, 2, 3, 4)")
	if short != long {
		t.Fatalf("varying IN lists must collapse equal: %q vs %q", short, long)
	}
}

func TestNormalize_CollapsesExistingPlaceholdersInLists(t *testing.T) {
	got := Normalize("SELECT * FROM t WHERE id IN (?, ?, ?)")
	want := "select * from t where id in (?)"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalize_SquashesWhitespace(t *testing.T) {
	got := Normalize("SELECT   id\n  FROM users\n\tWHERE active   =   1")
	want := "select id from users where active = ?"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalize_CollapsesValuesTuples(t *testing.T) {
	got := Normalize("INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)")
	want := "insert into t (a, b) values (?)"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalize_NamedAndPositionalBindings(t *testing.T) {
	got := Normalize("SELECT * FROM users WHERE id = :id AND name = $1")
	want := "select * from users where id = ? and name = ?"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalize_DoesNotMangleTrailingDigitIdentifiers(t *testing.T) {
	got := Normalize("SELECT column2 FROM table1 WHERE column2 = 5")
	if !strings.Contains(got, "column2") {
		t.Fatalf("identifier column2 was mangled: %q", got)
	}
	if !strings.Contains(got, "= ?") {
		t.Fatalf("literal not replaced: %q", got)
	}
}

func TestNormalize_StripsDecimalAndHexLiterals(t *testing.T) {
	got := Normalize("SELECT * FROM t WHERE price > 19.99 AND flag = 0xFF")
	want := "select * from t where price > ? and flag = ?"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalize_Empty(t *testing.T) {
	if got := Normalize(""); got != "" {
		t.Fatalf("empty input should normalize to empty, got %q", got)
	}
}
