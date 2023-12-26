package fnmposition

import (
	"sync"
	"time"

	investapi "github.com/tinkoff/invest-api-go-sdk"

	"github.com/evsamsonov/trengin/v2"
)

type Security struct {
	Board string
	Code  string
}

type Position struct {
	mtx          sync.Mutex
	position     *trengin.Position
	closed       chan trengin.Position
	stopLossID   int32
	takeProfitID int32
	orderTrades  []*investapi.OrderTrade
	security     Security
}

func NewPosition(
	pos *trengin.Position,
	security Security,
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

func (p *Position) SetStopLoss(id string, stopLoss float64) {
	p.stopLossID = id
	p.position.StopLoss = stopLoss
}

func (p *Position) SetTakeProfitID(id string, takeProfit float64) {
	p.takeProfitID = id
	p.position.TakeProfit = takeProfit
}

func (p *Position) AddOrderTrade(orderTrades ...*investapi.OrderTrade) {
	p.orderTrades = append(p.orderTrades, orderTrades...)
}

func (p *Position) AddCommission(val float64) {
	p.position.AddCommission(val)
}

func (p *Position) StopLossID() string {
	return p.stopLossID
}

func (p *Position) TakeProfitID() string {
	return p.takeProfitID
}

func (p *Position) Position() trengin.Position {
	return *p.position
}

func (p *Position) Instrument() *investapi.Instrument {
	return nil //todo
	//return p.instrument
}

func (p *Position) OrderTrades() []*investapi.OrderTrade {
	result := make([]*investapi.OrderTrade, len(p.orderTrades))
	copy(result, p.orderTrades)
	return p.orderTrades
}

func (p *Position) Close(closePrice float64) error {
	if err := p.position.Close(time.Now(), closePrice); err != nil {
		return err
	}
	p.closed <- *p.position
	p.stopLossID, p.takeProfitID = "", ""
	return nil
}
