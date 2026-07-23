package fnmbroker

import (
	"fmt"
	"strconv"

	"google.golang.org/genproto/googleapis/type/decimal"
)

func floatToDecimal(f float64) *decimal.Decimal {
	return &decimal.Decimal{Value: strconv.FormatFloat(f, 'f', -1, 64)}
}

func intToDecimal(i int64) *decimal.Decimal {
	return &decimal.Decimal{Value: strconv.FormatInt(i, 10)}
}

func decimalToFloat(d *decimal.Decimal) (float64, error) {
	if d == nil || d.Value == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(d.Value, 64)
	if err != nil {
		return 0, fmt.Errorf("parse decimal %q: %w", d.Value, err)
	}
	return v, nil
}

func mustDecimalToFloat(d *decimal.Decimal) float64 {
	v, _ := decimalToFloat(d)
	return v
}
