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
		stopID:   2,
		closed:   closed,
	}
	err = currentPosition.Close(123.45)
	assert.NoError(t, err)
	assert.Equal(t, int32(0), currentPosition.StopID())

	select {
	case pos := <-closed:
		assert.Equal(t, 123.45, pos.ClosePrice)
		assert.Equal(t, now, pos.CloseTime)
	default:
		t.Fatal("finamPosition not send")
	}
}
