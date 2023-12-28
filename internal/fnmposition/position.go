package fnmposition

import (
	"sync"
	"time"

	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"

	"github.com/evsamsonov/trengin/v2"
)

type Position struct {
	mtx          sync.Mutex
	position     *trengin.Position
	closed       chan trengin.Position
	stopLossID   int32
	takeProfitID int32
	security     *tradeapi.Security
}

func NewPosition(
	pos *trengin.Position,
	security *tradeapi.Security,
	stopLossID int32,
	takeProfitID int32,
	closed chan trengin.Position,
) *Position {
	return &Position{
		position:     pos,
		stopLossID:   stopLossID,
		takeProfitID: takeProfitID,
		closed:       closed,
		security:     security,
	}
}

func (p *Position) SetStopLoss(id int32, stopLoss float64) {
	p.stopLossID = id
	p.position.StopLoss = stopLoss
}

func (p *Position) SetTakeProfitID(id int32, takeProfit float64) {
	p.takeProfitID = id
	p.position.TakeProfit = takeProfit
}

func (p *Position) AddCommission(val float64) {
	p.position.AddCommission(val)
}

func (p *Position) StopLossID() int32 {
	return p.stopLossID
}

func (p *Position) TakeProfitID() int32 {
	return p.takeProfitID
}

func (p *Position) Position() trengin.Position {
	return *p.position
}

func (p *Position) Close(closePrice float64) error {
	if err := p.position.Close(time.Now(), closePrice); err != nil {
		return err
	}
	p.closed <- *p.position
	p.stopLossID, p.takeProfitID = 0, 0
	return nil
}
