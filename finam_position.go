package fnmbroker

import (
	"sync"
	"time"

	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
	"github.com/evsamsonov/trengin/v2"
)

type finamPosition struct {
	mtx      sync.Mutex
	position *trengin.Position
	closed   chan trengin.Position
	stopID   int32
	security *tradeapi.Security
	trades   []*tradeapi.TradeEvent
}

func newFinamPosition(
	pos *trengin.Position,
	security *tradeapi.Security,
	stopID int32,
	closed chan trengin.Position,
) *finamPosition {
	return &finamPosition{
		position: pos,
		stopID:   stopID,
		closed:   closed,
		security: security,
	}
}

func (p *finamPosition) SetStop(id int32, stopLoss, takeProfit float64) {
	p.stopID = id
	p.position.StopLoss = stopLoss
	p.position.TakeProfit = takeProfit
}

func (p *finamPosition) AddCommission(val float64) {
	p.position.AddCommission(val)
}

func (p *finamPosition) AddOrderTrade(trade *tradeapi.TradeEvent) {
	p.trades = append(p.trades, trade)
}

func (p *finamPosition) StopID() int32 {
	return p.stopID
}

func (p *finamPosition) Security() *tradeapi.Security {
	return p.security
}

func (p *finamPosition) Position() trengin.Position {
	return *p.position
}

func (p *finamPosition) Trades() []*tradeapi.TradeEvent {
	result := make([]*tradeapi.TradeEvent, len(p.trades))
	copy(result, p.trades)
	return result
}

func (p *finamPosition) Close(closePrice float64) error {
	if err := p.position.Close(time.Now(), closePrice); err != nil {
		return err
	}
	p.closed <- *p.position
	p.stopID = 0
	return nil
}
