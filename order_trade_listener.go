package fnmbroker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	finamclient "github.com/evsamsonov/FinamTradeGo/v2"
	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const sendTimeout = 5 * time.Second

type OrderTradeListener struct {
	clientID string
	token    string
	logger   *zap.Logger

	client     finamclient.IFinamClient
	mu         sync.RWMutex
	orderChans []chan *tradeapi.OrderEvent
	tradeChans []chan *tradeapi.TradeEvent
}

func NewOrderTradeListener(clientID, token string, logger *zap.Logger) *OrderTradeListener {
	return &OrderTradeListener{
		clientID: clientID,
		token:    token,
		logger:   logger,
	}
}

func (e *OrderTradeListener) Run(ctx context.Context) error {
	for {
		var err error
		e.client, err = finamclient.NewFinamClient(e.clientID, e.token, ctx)
		if err != nil {
			return fmt.Errorf("new finam client: %w", err)
		}

		if err := e.run(ctx); err != nil {
			e.logger.Error("Failed to subscribe. Recreate finam client...", zap.Error(err))
			if errors.Is(err, context.Canceled) {
				return err
			}
		}
	}
}

func (e *OrderTradeListener) run(ctx context.Context) error {
	requestID := uuid.New().String()[:16]
	go e.client.SubscribeOrderTrade(&tradeapi.OrderTradeSubscribeRequest{
		RequestId:     requestID,
		IncludeTrades: true,
		IncludeOrders: true,
		ClientIds:     []string{e.clientID},
	})
	defer e.close(requestID)

	errChan := e.client.GetErrorChan()
	orderChan := e.client.GetOrderChan()
	orderTradeChan := e.client.GetOrderTradeChan()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			return fmt.Errorf("subscribe order trade: %w", err)
		case <-time.After(1 * time.Minute):
			resp := e.client.SubscribeKeepAlive(&tradeapi.KeepAliveRequest{
				RequestId: uuid.New().String()[:16],
			})
			if !resp.Success {
				e.logger.Error("Failed to send keep alive", zap.Any("resp", resp))
				continue
			}
			e.logger.Debug("Keep alive response", zap.Any("resp", resp))
		case order := <-orderChan:
			if order == nil {
				e.logger.Debug("Nil order received")
				continue
			}
			e.logger.Debug("Order received", zap.Any("orders", order))

			e.sendOrders(ctx, order)
		case trade := <-orderTradeChan:
			if trade == nil {
				e.logger.Debug("Nil trade received")
				continue
			}
			e.logger.Debug("Trade received", zap.Any("orderTrade", trade))

			e.sendTrades(ctx, trade)
		}
	}
}

// unsubscribe third argument
func (e *OrderTradeListener) Subscribe() (<-chan *tradeapi.OrderEvent, <-chan *tradeapi.TradeEvent, func()) {
	orderChan := make(chan *tradeapi.OrderEvent)
	tradeChan := make(chan *tradeapi.TradeEvent)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.orderChans = append(e.orderChans, orderChan)
	e.tradeChans = append(e.tradeChans, tradeChan)

	return orderChan, tradeChan, func() {
		e.unsubscribe(orderChan)
	}
}

func (e *OrderTradeListener) sendOrders(ctx context.Context, order *tradeapi.OrderEvent) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, ch := range e.orderChans {
		go func(ch chan *tradeapi.OrderEvent) {
			select {
			case <-ctx.Done():
				return
			case ch <- order:
			case <-time.After(sendTimeout):
				e.logger.Error("Send order timeout", zap.Any("order", order))
			}
		}(ch)
	}
}

func (e *OrderTradeListener) sendTrades(ctx context.Context, trade *tradeapi.TradeEvent) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, ch := range e.tradeChans {
		go func(ch chan *tradeapi.TradeEvent) {
			select {
			case <-ctx.Done():
				return
			case ch <- trade:
			case <-time.After(sendTimeout):
				e.logger.Error("Send trade timeout", zap.Any("trade", trade))
			}
		}(ch)
	}
}

func (e *OrderTradeListener) unsubscribe(orderChan <-chan *tradeapi.OrderEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, ch := range e.orderChans {
		if orderChan != ch {
			continue
		}
		e.orderChans = append(e.orderChans[:i], e.orderChans[i+1:]...)
		e.tradeChans = append(e.tradeChans[:i], e.tradeChans[i+1:]...)
		break
	}
}

func (e *OrderTradeListener) close(requestID string) { //nolint: unparam
	// todo понять почему зависаем
	/*resp := e.client.UnSubscribeOrderTrade(&tradeapi.OrderTradeUnsubscribeRequest{
		RequestId: requestID,
	})
	if !resp.Success {
		e.logger.Error("Failed to unsubscribe order trade", zap.Any("errors", resp.Errors))
	}*/

	e.mu.Lock()
	defer e.mu.Unlock()

	for i := 0; i < len(e.orderChans); i++ {
		close(e.orderChans[i])
		close(e.tradeChans[i])
	}
}
