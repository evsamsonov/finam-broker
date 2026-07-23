package fnmbroker

import (
	"sync"
	"time"

	tradeapi "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/evsamsonov/trengin/v2"
)

type finamPosition struct {
	position *trengin.Position
	security *security
	stopID   string
	closed   chan trengin.Position

	mu     sync.Mutex
	trades []*tradeapi.AccountTrade
}

func newFinamPosition(
	position *trengin.Position,
	sec *security,
	stopID string,
	closed chan trengin.Position,
) *finamPosition {
	return &finamPosition{
		position: position,
		stopID:   stopID,
		closed:   closed,
		security: sec,
	}
}

func (p *finamPosition) SetStop(id string, stopLoss, takeProfit float64) {
	p.stopID = id
	p.position.StopLoss = stopLoss
	p.position.TakeProfit = takeProfit
}

func (p *finamPosition) AddCommission(val float64) {
	p.position.AddCommission(val)
}

func (p *finamPosition) AddOrderTrade(trade *tradeapi.AccountTrade) {
	p.trades = append(p.trades, trade)
}

func (p *finamPosition) StopID() string {
	return p.stopID
}

func (p *finamPosition) Security() *security {
	return p.security
}

func (p *finamPosition) Position() trengin.Position {
	return *p.position
}

func (p *finamPosition) Trades() []*tradeapi.AccountTrade {
	result := make([]*tradeapi.AccountTrade, len(p.trades))
	copy(result, p.trades)
	return result
}

func (p *finamPosition) Close(closePrice float64) error {
	if err := p.position.Close(time.Now(), closePrice); err != nil {
		return err
	}
	p.closed <- *p.position
	p.stopID = ""
	return nil
}
