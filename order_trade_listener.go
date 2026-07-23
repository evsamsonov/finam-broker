package fnmbroker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	tradeapi "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	orderTradeSendTimeout              = 5 * time.Second
	orderTradeSubscriptionRetryTimeout = 5 * time.Second
)

type orderTradeListener struct {
	client *finamAPIClient
	logger *zap.Logger

	mu         sync.RWMutex
	orderChans []chan *orders.OrderState
	tradeChans []chan *tradeapi.AccountTrade
}

func newOrderTradeListener(client *finamAPIClient, logger *zap.Logger) *orderTradeListener {
	return &orderTradeListener{
		client: client,
		logger: logger,
	}
}

func (o *orderTradeListener) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return o.runOrders(ctx)
	})
	g.Go(func() error {
		return o.runTrades(ctx)
	})
	return g.Wait()
}

// Subscribe subscribes to order and trade events.
// Unsubscribe function is the third return value.
func (o *orderTradeListener) Subscribe() (
	<-chan *orders.OrderState,
	<-chan *tradeapi.AccountTrade,
	func(),
) {
	orderChan := make(chan *orders.OrderState)
	tradeChan := make(chan *tradeapi.AccountTrade)

	o.mu.Lock()
	defer o.mu.Unlock()
	o.orderChans = append(o.orderChans, orderChan)
	o.tradeChans = append(o.tradeChans, tradeChan)

	return orderChan, tradeChan, func() {
		o.unsubscribe(orderChan)
	}
}

func (o *orderTradeListener) runOrders(ctx context.Context) error {
	for {
		if err := o.readOrders(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			o.logger.Warn("Failed to read orders. Retry", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(orderTradeSubscriptionRetryTimeout):
		}
	}
}

func (o *orderTradeListener) runTrades(ctx context.Context) error {
	for {
		if err := o.readTrades(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			o.logger.Warn("Failed to read trades. Retry", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(orderTradeSubscriptionRetryTimeout):
		}
	}
}

func (o *orderTradeListener) readOrders(ctx context.Context) error {
	stream, err := o.client.orders.SubscribeOrders(ctx, &orders.SubscribeOrdersRequest{
		AccountId: o.client.accountID,
	})
	if err != nil {
		return fmt.Errorf("subscribe orders: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("recv orders: %w", err)
		}
		for _, order := range resp.GetOrders() {
			o.logger.Debug("Order received", zap.Any("order", order))
			o.sendOrders(ctx, order)
		}
	}
}

func (o *orderTradeListener) readTrades(ctx context.Context) error {
	stream, err := o.client.orders.SubscribeTrades(ctx, &orders.SubscribeTradesRequest{
		AccountId: o.client.accountID,
	})
	if err != nil {
		return fmt.Errorf("subscribe trades: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("recv trades: %w", err)
		}
		for _, trade := range resp.GetTrades() {
			o.logger.Debug("Trade received", zap.Any("trade", trade))
			o.sendTrades(ctx, trade)
		}
	}
}

func (o *orderTradeListener) sendOrders(ctx context.Context, order *orders.OrderState) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, ch := range o.orderChans {
		select {
		case <-ctx.Done():
			return
		case ch <- order:
		case <-time.After(orderTradeSendTimeout):
			o.logger.Error("Send order timeout", zap.Any("order", order))
		}
	}
}

func (o *orderTradeListener) sendTrades(ctx context.Context, trade *tradeapi.AccountTrade) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, ch := range o.tradeChans {
		select {
		case <-ctx.Done():
			return
		case ch <- trade:
		case <-time.After(orderTradeSendTimeout):
			o.logger.Error("Send trade timeout", zap.Any("trade", trade))
		}
	}
}

func (o *orderTradeListener) unsubscribe(orderChan <-chan *orders.OrderState) {
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
