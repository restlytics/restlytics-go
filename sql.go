package restlytics

import (
	"regexp"
	"strings"
)

// SQL normalization → a literal-free template string.
//
// Two jobs:
//  1. PII / redaction — strip every literal so we NEVER ship customer values
//     (emails, tokens, ids) inside `db.query.summary`. Only the shape survives.
//  2. N+1 grouping — collapse the query down to a stable fingerprint so that
//     `SELECT * FROM users WHERE id = 1` and `... id = 2` map to the same key.
//     `IN (?, ?, ?)` lists of varying length also collapse to `IN (?)` so a
//     batched query and its single-row cousin don't fragment the grouping.
//
// This is deliberately a best-effort lexical normalizer, not a real SQL parser —
// it must be fast (runs on every query) and never panic.

var (
	reStringSingle = regexp.MustCompile(`'(?:[^'\\]|\\.|'')*'`)
	reStringDouble = regexp.MustCompile(`"(?:[^"\\]|\\.|"")*"`)

	reHex       = regexp.MustCompile(`\b0x[0-9a-fA-F]+\b`)
	reDecimal   = regexp.MustCompile(`\b\d+\.\d+(?:[eE][+-]?\d+)?\b`)
	reInt       = regexp.MustCompile(`\b\d+\b`)
	rePlacehNum = regexp.MustCompile(`\?\d+`)
	reNamed     = regexp.MustCompile(`[:$]\w+`)

	reInList     = regexp.MustCompile(`(?i)\bin\s*\(\s*\?(?:\s*,\s*\?)*\s*\)`)
	reTuplesMany = regexp.MustCompile(`\(\s*\?(?:\s*,\s*\?)*\s*\)(?:\s*,\s*\(\s*\?(?:\s*,\s*\?)*\s*\))+`)
	reTupleMulti = regexp.MustCompile(`\(\s*\?(?:\s*,\s*\?)+\s*\)`)

	reWhitespace = regexp.MustCompile(`\s+`)
)

// Normalize turns a raw SQL string into a stable, literal-free template.
func Normalize(sql string) string {
	s := sql

	// Drop string literals: single- and double-quoted, with escaped-quote support.
	s = reStringSingle.ReplaceAllString(s, "?")
	s = reStringDouble.ReplaceAllString(s, "?")

	// Normalize named/positional placeholders FIRST, as whole tokens, so the
	// numeric pass below doesn't bite the digit out of `$1` and leave a stray `$`.
	s = reNamed.ReplaceAllString(s, "?")     // :name, $1
	s = rePlacehNum.ReplaceAllString(s, "?") // ?1, ?2 (some drivers)

	// Drop numeric literals (hex, decimal, int). Word boundaries keep identifiers
	// like `column2` intact.
	s = reHex.ReplaceAllString(s, "?")
	s = reDecimal.ReplaceAllString(s, "?")
	s = reInt.ReplaceAllString(s, "?")

	// Collapse `IN (?, ?, ?)` → `IN (?)` so list length doesn't fragment groups.
	s = reInList.ReplaceAllString(s, "IN (?)")

	// Collapse multi-row VALUES tuples: (?, ?), (?, ?) → (?)
	s = reTuplesMany.ReplaceAllString(s, "(?)")
	s = reTupleMulti.ReplaceAllString(s, "(?)")

	// Squash all whitespace runs into single spaces, then trim.
	s = reWhitespace.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)

	// Lowercase so casing differences don't fragment the grouping key.
	return strings.ToLower(s)
}
