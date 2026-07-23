package fnmbroker

import (
	"testing"
	"time"

	"github.com/evsamsonov/trengin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/undefinedlabs/go-mpatch"
)

func Test_currentPosition_Close(t *testing.T) {
	pos, err := trengin.NewPosition(
		trengin.OpenPositionAction{
			Type:     trengin.Long,
			Quantity: 2,
		},
		time.Now(),
		1,
	)
	assert.NoError(t, err)

	now := time.Now()
	patch, err := mpatch.PatchMethod(time.Now, func() time.Time {
		return now
	})
	defer func() { assert.NoError(t, patch.Unpatch()) }()
	assert.NoError(t, err)

	closed := make(chan trengin.Position, 1)
	currentPosition := finamPosition{
		position: pos,
		stopID:   "stop-2",
		closed:   closed,
	}
	err = currentPosition.Close(123.45)
	assert.NoError(t, err)
	assert.Equal(t, "", currentPosition.StopID())

	select {
	case pos := <-closed:
		assert.Equal(t, 123.45, pos.ClosePrice)
		assert.Equal(t, now, pos.CloseTime)
	default:
		t.Fatal("finamPosition not send")
	}
}

func Test_sameInstrument(t *testing.T) {
	sec := &security{Symbol: "SBER@MISX", Code: "SBER", Board: "TQBR", Mic: "MISX"}
	assert.True(t, sameInstrument("SBER@MISX", sec))
	assert.True(t, sameInstrument("SBER@FOO", sec))
	assert.False(t, sameInstrument("GAZP@MISX", sec))
	assert.False(t, sameInstrument("", sec))
}

func Test_security_pieces_lots(t *testing.T) {
	sec := &security{LotSize: 10}
	assert.Equal(t, int64(20), sec.pieces(2))
	assert.Equal(t, int64(2), sec.lots(20))

	secZero := &security{}
	assert.Equal(t, int64(3), secZero.pieces(3))
	assert.Equal(t, int64(7), secZero.lots(7))
}

func Test_decimalHelpers(t *testing.T) {
	d := floatToDecimal(12.5)
	v, err := decimalToFloat(d)
	assert.NoError(t, err)
	assert.Equal(t, 12.5, v)

	i := intToDecimal(42)
	assert.Equal(t, "42", i.GetValue())
	assert.Equal(t, float64(0), mustDecimalToFloat(nil))
}
