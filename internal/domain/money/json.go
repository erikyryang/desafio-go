package money

import (
	"encoding/json"
	"fmt"
)

// JSON wire form: {"amount":"25.00","currency":"BRL"}.
type jsonMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON serializes the value as {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(jsonMoney{Amount: m.Amount(), Currency: string(m.currency)})
}

// UnmarshalJSON parses {"amount":"25.00","currency":"BRL"}. Amounts must be
// strings: numeric JSON literals are rejected so that no float parsing occurs.
func (m *Money) UnmarshalJSON(data []byte) error {
	var raw struct {
		Amount   json.RawMessage `json:"amount"`
		Currency string          `json:"currency"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	var amount string
	if err := json.Unmarshal(raw.Amount, &amount); err != nil {
		return fmt.Errorf("%w: amount must be a decimal string", ErrInvalidAmount)
	}
	parsed, err := Parse(amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
