// Package money implements an immutable monetary value object with exact
// integer arithmetic. Amounts are stored as int64 minor units with a fixed
// scale of two decimal places (ISO 4217 minor units for BRL, USD, EUR...).
//
// Limits: the representable range is [-92233720368547758.08, 92233720368547758.07].
// Every operation that could overflow (parsing, Add, Sub, Negate) reports
// ErrOverflow instead of wrapping silently.
package money

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// Scale is the number of decimal places carried by every Money value.
const Scale = 2

// scaleFactor is 10^Scale.
const scaleFactor int64 = 100

var (
	// ErrInvalidAmount is returned when a string cannot be parsed as a decimal amount.
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrScaleExceeded is returned when the input has more than Scale decimal places.
	ErrScaleExceeded = errors.New("money: scale exceeded")
	// ErrInvalidCurrency is returned for a malformed ISO 4217 currency code.
	ErrInvalidCurrency = errors.New("money: invalid currency")
	// ErrCurrencyMismatch is returned when two values of different currencies are combined.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrOverflow is returned when an operation exceeds the int64 range.
	ErrOverflow = errors.New("money: overflow")
	// ErrNegative is returned when a negative amount is not allowed.
	ErrNegative = errors.New("money: negative amount not allowed")
	// ErrUninitialized is returned when a zero-value Money is used.
	ErrUninitialized = errors.New("money: uninitialized value")
)

// Currency is an ISO 4217 alphabetic code (three uppercase ASCII letters).
type Currency string

// BRL is the currency used by the challenge's main scenarios.
const BRL Currency = "BRL"

// ParseCurrency validates a currency code. Only the syntactic form is checked;
// the application decides which currencies it accepts.
func ParseCurrency(s string) (Currency, error) {
	if len(s) != 3 {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, s)
	}
	for i := 0; i < 3; i++ {
		if s[i] < 'A' || s[i] > 'Z' {
			return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, s)
		}
	}
	return Currency(s), nil
}

// String returns the code.
func (c Currency) String() string { return string(c) }

// IsValid reports whether the currency has a well-formed code.
func (c Currency) IsValid() bool {
	_, err := ParseCurrency(string(c))
	return err == nil
}

// Money is an immutable amount in a currency. The zero value is invalid; use
// Zero, Parse or FromMinor to build values.
type Money struct {
	minor    int64
	currency Currency
	valid    bool
}

// Zero returns 0.00 in the given currency.
func Zero(c Currency) (Money, error) {
	if !c.IsValid() {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, string(c))
	}
	return Money{minor: 0, currency: c, valid: true}, nil
}

// MustZero is Zero for statically-known currencies; it panics on invalid input.
func MustZero(c Currency) Money {
	m, err := Zero(c)
	if err != nil {
		panic(err)
	}
	return m
}

// FromMinor builds a Money from minor units (e.g. cents). Negative values are
// allowed here because internal calculations (differences) may be negative.
func FromMinor(minor int64, c Currency) (Money, error) {
	if !c.IsValid() {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, string(c))
	}
	return Money{minor: minor, currency: c, valid: true}, nil
}

// Parse parses a decimal string such as "25.00", "25.5" or "-3" into Money.
//
// Accepted grammar: ^-?[0-9]{1,17}(\.[0-9]{1,2})?$ (a leading minus sign is
// accepted; callers that need non-negative input use ParseNonNegative).
// Rejected: empty strings, whitespace, "+", NaN, Infinity, scientific notation,
// thousand separators, more than two decimal places and values outside the
// int64 range. "25" and "25.5" are normalized to "25.00" and "25.50".
func Parse(amount string, currency string) (Money, error) {
	c, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c, valid: true}, nil
}

// ParseNonNegative is Parse but rejects negative input (external boundary).
func ParseNonNegative(amount string, currency string) (Money, error) {
	m, err := Parse(amount, currency)
	if err != nil {
		return Money{}, err
	}
	if m.minor < 0 {
		return Money{}, fmt.Errorf("%w: %q", ErrNegative, amount)
	}
	return m, nil
}

// MustParse is Parse for constants in tests and fixtures; it panics on error.
func MustParse(amount string, currency string) Money {
	m, err := Parse(amount, currency)
	if err != nil {
		panic(err)
	}
	return m
}

func parseMinor(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	if intPart == "" || !allDigits(intPart) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if hasDot {
		if fracPart == "" || !allDigits(fracPart) {
			return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
		}
		if len(fracPart) > Scale {
			return 0, fmt.Errorf("%w: %q has more than %d decimal places", ErrScaleExceeded, s, Scale)
		}
	}
	// 17 integer digits already exceed int64 minor units (max ~9.2e16 integer part).
	if len(intPart) > 17 {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
	}
	var minor int64
	for i := 0; i < len(intPart); i++ {
		d := int64(intPart[i] - '0')
		if minor > (math.MaxInt64-d)/10 {
			return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
		}
		minor = minor*10 + d
	}
	for i := 0; i < Scale; i++ {
		d := int64(0)
		if i < len(fracPart) {
			d = int64(fracPart[i] - '0')
		}
		if minor > (math.MaxInt64-d)/10 {
			return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
		}
		minor = minor*10 + d
	}
	if neg {
		minor = -minor
	}
	return minor, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// IsValid reports whether the value was built through a constructor.
func (m Money) IsValid() bool { return m.valid && m.currency.IsValid() }

// Minor returns the amount in minor units.
func (m Money) Minor() int64 { return m.minor }

// Currency returns the currency code.
func (m Money) Currency() Currency { return m.currency }

// Amount returns the decimal representation with exactly two decimal places,
// e.g. "25.00" or "-0.50".
func (m Money) Amount() string {
	return formatMinor(m.minor)
}

// String returns "<amount> <currency>", e.g. "25.00 BRL".
func (m Money) String() string {
	if !m.valid {
		return "<invalid money>"
	}
	return m.Amount() + " " + string(m.currency)
}

func formatMinor(minor int64) string {
	neg := minor < 0
	var abs uint64
	if neg {
		abs = uint64(-(minor + 1)) + 1 // safe for MinInt64
	} else {
		abs = uint64(minor)
	}
	whole := abs / uint64(scaleFactor)
	frac := abs % uint64(scaleFactor)
	s := fmt.Sprintf("%d.%02d", whole, frac)
	if neg {
		return "-" + s
	}
	return s
}

func (m Money) check(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add returns m + o.
func (m Money) Add(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	sum := m.minor + o.minor
	// Overflow iff both operands share a sign and the result's sign differs.
	if (m.minor > 0 && o.minor > 0 && sum < 0) || (m.minor < 0 && o.minor < 0 && sum >= 0) {
		return Money{}, ErrOverflow
	}
	return Money{minor: sum, currency: m.currency, valid: true}, nil
}

// Sub returns m - o.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	diff := m.minor - o.minor
	if (o.minor < 0 && diff < m.minor) || (o.minor > 0 && diff > m.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: diff, currency: m.currency, valid: true}, nil
}

// Negate returns -m.
func (m Money) Negate() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrUninitialized
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency, valid: true}, nil
}

// Cmp compares two values of the same currency: -1, 0 or +1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.check(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether both values have the same amount and currency.
// Values of different currencies are never equal.
func (m Money) Equal(o Money) bool {
	return m.valid && o.valid && m.currency == o.currency && m.minor == o.minor
}

// IsZero reports whether the amount is 0.
func (m Money) IsZero() bool { return m.valid && m.minor == 0 }

// IsPositive reports whether the amount is > 0.
func (m Money) IsPositive() bool { return m.valid && m.minor > 0 }

// IsNegative reports whether the amount is < 0.
func (m Money) IsNegative() bool { return m.valid && m.minor < 0 }

// SameCurrency reports whether both values share a currency.
func (m Money) SameCurrency(o Money) bool { return m.valid && o.valid && m.currency == o.currency }
