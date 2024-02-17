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
	"golang.org/x/sync/errgroup"
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
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer cancel()
		o.logger.Debug("Start main subscription")

		return o.run(ctx)
	})

	// todo why need redundant subscription
	g.Go(func() error {
		defer cancel()
		<-time.After(5 * time.Minute)
		o.logger.Debug("Start redundant subscription")

		return o.run(ctx)
	})
	return g.Wait()
}

// todo unsubscribe third argument
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

func (o *orderTradeListener) run(ctx context.Context) error {
	for {
		client, err := finamclient.NewFinamClient(o.clientID, o.token, ctx)
		if err != nil {
			o.logger.Error("Failed to create finam client", zap.Error(err))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(orderTradeSubscriptionRetryTimeout):
			}
			continue
		}

		if err := o.readOrderTrade(ctx, client); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			o.logger.Error(
				"Failed to read order trade. Retry",
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

func (o *orderTradeListener) readOrderTrade(ctx context.Context, client finamclient.IFinamClient) error {
	requestID := uuid.New().String()[:16]
	go client.SubscribeOrderTrade(&tradeapi.OrderTradeSubscribeRequest{
		RequestId:     requestID,
		IncludeTrades: true,
		IncludeOrders: true,
		ClientIds:     []string{o.clientID},
	})

	errChan := client.GetErrorChan()
	orderChan := client.GetOrderChan()
	orderTradeChan := client.GetOrderTradeChan()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			return fmt.Errorf("read order trade: %w", err)
		case <-time.After(orderTradeKeepAliveTimeout):
			resp := client.SubscribeKeepAlive(&tradeapi.KeepAliveRequest{
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
