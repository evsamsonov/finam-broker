package fnmbroker

import (
	"sync"
	"time"

	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"

	"github.com/evsamsonov/trengin/v2"
)

type finamPosition struct {
	mtx          sync.Mutex
	position     *trengin.Position
	closed       chan trengin.Position
	stopLossID   int32
	takeProfitID int32
	security     *tradeapi.Security
}

func newFinamPosition(
	pos *trengin.Position,
	security *tradeapi.Security,
	stopLossID int32,
	takeProfitID int32,
	closed chan trengin.Position,
) *finamPosition {
	return &finamPosition{
		position:     pos,
		stopLossID:   stopLossID,
		takeProfitID: takeProfitID,
		closed:       closed,
		security:     security,
	}
}

func (p *finamPosition) SetStopLoss(id int32, stopLoss float64) {
	p.stopLossID = id
	p.position.StopLoss = stopLoss
}

func (p *finamPosition) SetTakeProfitID(id int32, takeProfit float64) {
	p.takeProfitID = id
	p.position.TakeProfit = takeProfit
}

func (p *finamPosition) AddCommission(val float64) {
	p.position.AddCommission(val)
}

func (p *finamPosition) StopLossID() int32 {
	return p.stopLossID
}

func (p *finamPosition) TakeProfitID() int32 {
	return p.takeProfitID
}

func (p *finamPosition) Security() *tradeapi.Security {
	return p.security
}

func (p *finamPosition) Position() trengin.Position {
	return *p.position
}

func (p *finamPosition) Close(closePrice float64) error {
	if err := p.position.Close(time.Now(), closePrice); err != nil {
		return err
	}
	p.closed <- *p.position
	p.stopLossID, p.takeProfitID = 0, 0
	return nil
}
