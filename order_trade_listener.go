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

const (
	orderTradeSendTimeout              = 5 * time.Second
	orderTradeKeepAliveTimeout         = 1 * time.Minute
	orderTradeSubscriptionRetryTimeout = 5 * time.Second
)

type orderTradeListener struct {
	clientID string
	token    string
	logger   *zap.Logger

	client     finamclient.IFinamClient
	mu         sync.RWMutex
	orderChans []chan *tradeapi.OrderEvent
	tradeChans []chan *tradeapi.TradeEvent
}

func newOrderTradeListener(clientID, token string, logger *zap.Logger) *orderTradeListener {
	return &orderTradeListener{
		clientID: clientID,
		token:    token,
		logger:   logger,
	}
}

func (o *orderTradeListener) Run(ctx context.Context) error {
	for {
		var err error
		o.client, err = finamclient.NewFinamClient(o.clientID, o.token, ctx)
		if err != nil {
			return fmt.Errorf("new finam client: %w", err)
		}

		if err := o.run(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			o.logger.Error(
				"Failed to read order trade. Recreate finam client and subscribe again...",
				zap.Error(err),
			)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(orderTradeSubscriptionRetryTimeout):
			}
		}
	}
}

func (o *orderTradeListener) run(ctx context.Context) error {
	requestID := uuid.New().String()[:16]
	go o.client.SubscribeOrderTrade(&tradeapi.OrderTradeSubscribeRequest{
		RequestId:     requestID,
		IncludeTrades: true,
		IncludeOrders: true,
		ClientIds:     []string{o.clientID},
	})

	errChan := o.client.GetErrorChan()
	orderChan := o.client.GetOrderChan()
	orderTradeChan := o.client.GetOrderTradeChan()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			return fmt.Errorf("subscribe order trade: %w", err)
		case <-time.After(orderTradeKeepAliveTimeout):
			resp := o.client.SubscribeKeepAlive(&tradeapi.KeepAliveRequest{
				RequestId: uuid.New().String()[:16],
			})
			if !resp.Success {
				o.logger.Error("Failed to send keep alive", zap.Any("resp", resp))
				continue
			}
			o.logger.Debug("Keep alive response", zap.Any("resp", resp))
		case order := <-orderChan:
			o.logger.Debug("Order received", zap.Any("orders", order))

			o.sendOrders(ctx, order)
		case trade := <-orderTradeChan:
			o.logger.Debug("Trade received", zap.Any("orderTrade", trade))

			o.sendTrades(ctx, trade)
		}
	}
}

// unsubscribe third argument
func (o *orderTradeListener) Subscribe() (<-chan *tradeapi.OrderEvent, <-chan *tradeapi.TradeEvent, func()) {
	orderChan := make(chan *tradeapi.OrderEvent)
	tradeChan := make(chan *tradeapi.TradeEvent)

	o.mu.Lock()
	defer o.mu.Unlock()
	o.orderChans = append(o.orderChans, orderChan)
	o.tradeChans = append(o.tradeChans, tradeChan)

	return orderChan, tradeChan, func() {
		o.unsubscribe(orderChan)
	}
}

func (o *orderTradeListener) sendOrders(ctx context.Context, order *tradeapi.OrderEvent) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, ch := range o.orderChans {
		go func(ch chan *tradeapi.OrderEvent) {
			select {
			case <-ctx.Done():
				return
			case ch <- order:
			case <-time.After(orderTradeSendTimeout):
				o.logger.Error("Send order timeout", zap.Any("order", order))
			}
		}(ch)
	}
}

func (o *orderTradeListener) sendTrades(ctx context.Context, trade *tradeapi.TradeEvent) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, ch := range o.tradeChans {
		go func(ch chan *tradeapi.TradeEvent) {
			select {
			case <-ctx.Done():
				return
			case ch <- trade:
			case <-time.After(orderTradeSendTimeout):
				o.logger.Error("Send trade timeout", zap.Any("trade", trade))
			}
		}(ch)
	}
}

func (o *orderTradeListener) unsubscribe(orderChan <-chan *tradeapi.OrderEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for i, ch := range o.orderChans {
		if orderChan != ch {
			continue
		}
		o.orderChans = append(o.orderChans[:i], o.orderChans[i+1:]...)
		o.tradeChans = append(o.tradeChans[:i], o.tradeChans[i+1:]...)
		break
	}
}
