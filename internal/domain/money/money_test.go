package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseValid(t *testing.T) {
	cases := map[string]string{"25.00": "25.00", "25": "25.00", "25.5": "25.50", "0": "0.00", "0.01": "0.01",
		"-3": "-3.00", "-0.50": "-0.50", "92233720368547758.07": "92233720368547758.07"}
	for in, want := range cases {
		m, err := Parse(in, "BRL")
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if m.Amount() != want {
			t.Errorf("Parse(%q).Amount() = %q, want %q", in, m.Amount(), want)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	cases := map[string]error{
		"": ErrInvalidAmount, " 1": ErrInvalidAmount, "1 ": ErrInvalidAmount, "+1": ErrInvalidAmount,
		"NaN": ErrInvalidAmount, "Infinity": ErrInvalidAmount, "-Infinity": ErrInvalidAmount, "1e3": ErrInvalidAmount,
		"1E3": ErrInvalidAmount, "1,00": ErrInvalidAmount, "1.": ErrInvalidAmount, ".5": ErrInvalidAmount,
		"0x10": ErrInvalidAmount, "1.000": ErrScaleExceeded, "25.005": ErrScaleExceeded,
		"92233720368547758.08": ErrOverflow, "999999999999999999": ErrOverflow, "100000000000000000": ErrOverflow,
	}
	for in, want := range cases {
		_, err := Parse(in, "BRL")
		if !errors.Is(err, want) {
			t.Errorf("Parse(%q) err = %v, want %v", in, err, want)
		}
	}
	if _, err := Parse("1.00", "brl"); !errors.Is(err, ErrInvalidCurrency) {
		t.Errorf("lowercase currency accepted: %v", err)
	}
	if _, err := Parse("1.00", "BRLX"); !errors.Is(err, ErrInvalidCurrency) {
		t.Errorf("4-letter currency accepted: %v", err)
	}
	if _, err := ParseNonNegative("-1.00", "BRL"); !errors.Is(err, ErrNegative) {
		t.Errorf("negative accepted by ParseNonNegative: %v", err)
	}
}

func TestArithmetic(t *testing.T) {
	a, b := MustParse("10.25", "BRL"), MustParse("0.75", "BRL")
	sum, _ := a.Add(b)
	if sum.Amount() != "11.00" {
		t.Errorf("Add = %s", sum.Amount())
	}
	diff, _ := b.Sub(a)
	if diff.Amount() != "-9.50" || !diff.IsNegative() {
		t.Errorf("Sub = %s", diff.Amount())
	}
	neg, _ := a.Negate()
	if neg.Amount() != "-10.25" {
		t.Errorf("Negate = %s", neg.Amount())
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Errorf("Cmp = %d", c)
	}
	if !a.Equal(MustParse("10.25", "BRL")) || a.Equal(MustParse("10.25", "USD")) {
		t.Error("Equal mismatch")
	}
	z := MustZero("BRL")
	if !z.IsZero() || z.IsPositive() {
		t.Error("zero flags")
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl, usd := MustParse("1.00", "BRL"), MustParse("1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Cmp: %v", err)
	}
}

func TestOverflow(t *testing.T) {
	max, _ := FromMinor(math.MaxInt64, "BRL")
	min, _ := FromMinor(math.MinInt64, "BRL")
	one := MustParse("0.01", "BRL")
	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("Add overflow: %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("Sub overflow: %v", err)
	}
	if _, err := min.Negate(); !errors.Is(err, ErrOverflow) {
		t.Errorf("Negate overflow: %v", err)
	}
	if min.Amount() != "-92233720368547758.08" {
		t.Errorf("format MinInt64 = %s", min.Amount())
	}
}

func TestUninitialized(t *testing.T) {
	var z Money
	if z.IsValid() {
		t.Fatal("zero value must be invalid")
	}
	if _, err := z.Add(MustParse("1", "BRL")); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Add: %v", err)
	}
	if _, err := json.Marshal(z); err == nil {
		t.Error("marshal of zero value must fail")
	}
}

func TestJSON(t *testing.T) {
	var m Money
	if err := json.Unmarshal([]byte(`{"amount":"25.00","currency":"BRL"}`), &m); err != nil || m.Amount() != "25.00" {
		t.Fatalf("unmarshal: %v %s", err, m)
	}
	out, _ := json.Marshal(m)
	if string(out) != `{"amount":"25.00","currency":"BRL"}` {
		t.Errorf("marshal = %s", out)
	}
	for _, bad := range []string{`{"amount":25.00,"currency":"BRL"}`, `{"amount":"25.001","currency":"BRL"}`, `{"amount":"1e2","currency":"BRL"}`, `{"amount":"NaN","currency":"BRL"}`, `{"amount":"1","currency":""}`} {
		if err := json.Unmarshal([]byte(bad), &m); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}
