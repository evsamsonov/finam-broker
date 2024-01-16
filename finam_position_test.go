package fnmbroker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/undefinedlabs/go-mpatch"

	"github.com/evsamsonov/trengin/v2"
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
		position:     pos,
		stopLossID:   2,
		takeProfitID: 3,
		closed:       closed,
	}
	err = currentPosition.Close(123.45)
	assert.NoError(t, err)
	assert.Equal(t, "", currentPosition.StopLossID())
	assert.Equal(t, "", currentPosition.TakeProfitID())

	select {
	case pos := <-closed:
		assert.Equal(t, 123.45, pos.ClosePrice)
		assert.Equal(t, now, pos.CloseTime)
	default:
		t.Fatal("finamPosition not send")
	}
}
