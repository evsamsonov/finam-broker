// Package fnmbroker implements [trengin.Broker] using [Finam Trade API].
//
// [Finam Trade API]: https://finamweb.github.io/trade-api-docs/
package fnmbroker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"golang.org/x/sync/errgroup"

	finamclient "github.com/evsamsonov/FinamTradeGo/v2"
	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
	"github.com/evsamsonov/trengin/v2"
	"go.uber.org/zap"
)

var _ trengin.Broker = &Finam{}

const (
	defaultProtectiveSpreadPercent = 1
	defaultUseCredit               = true
)

type Finam struct {
	clientID string
	token    string
	logger   *zap.Logger

	client                  finamclient.IFinamClient
	positionStorage         *positionStorage
	orderTradeListener      *orderTradeListener
	securityProvider        securityProvider
	protectiveSpreadPercent float64
	useCredit               bool
}

type Option func(*Finam)

// WithLogger returns Option which sets logger. The default logger is no-op Logger
func WithLogger(logger *zap.Logger) Option {
	return func(f *Finam) {
		f.logger = logger
	}
}

// WithProtectiveSpreadPercent returns Option which sets protective spread
// in percent for executing orders. The default value is 1%
// todo использовать при открытии позиции
func WithProtectiveSpreadPercent(protectiveSpread float64) Option {
	return func(f *Finam) {
		f.protectiveSpreadPercent = protectiveSpread
	}
}

// WithUseCredit returns Option which sets using credit funds for executing orders.
// The default value is true
func WithUseCredit(useCredit bool) Option {
	return func(f *Finam) {
		f.useCredit = useCredit
	}
}

// New creates a new Finam object. It takes [full-access token], client id.
//
// [full-access token]: https://finamweb.github.io/trade-api-docs/tokens
func New(token, clientID string, opts ...Option) *Finam {
	finam := &Finam{
		clientID:                clientID,
		token:                   token,
		logger:                  zap.NewNop(),
		positionStorage:         newPositionStorage(),
		protectiveSpreadPercent: defaultProtectiveSpreadPercent,
		useCredit:               defaultUseCredit,
	}
	for _, opt := range opts {
		opt(finam)
	}
	return finam
}

// Run creates Finam client and starts to track an open positions
func (f *Finam) Run(ctx context.Context) error {
	finamClient, err := finamclient.NewFinamClient(f.clientID, f.token, ctx)
	if err != nil {
		return fmt.Errorf("new finam client: %w", err)
	}
	f.client = finamClient

	f.securityProvider, err = newSecurityProvider(finamClient, f.logger)
	if err != nil {
		return fmt.Errorf("get securities: %w", err)
	}

	f.orderTradeListener = newOrderTradeListener(
		f.clientID,
		f.token,
		f.logger,
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := f.orderTradeListener.Run(ctx); err != nil {
			return fmt.Errorf("order trade listener: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := f.trackOpenPosition(ctx); err != nil {
			return fmt.Errorf("track open position: %w", err)
		}
		return nil
	})

	return g.Wait()
}

// OpenPosition
// see https://finamweb.github.io/trade-api-docs/grpc/orders
func (f *Finam) OpenPosition(
	ctx context.Context,
	action trengin.OpenPositionAction,
) (trengin.Position, trengin.PositionClosed, error) {
	security, err := f.securityProvider.Get(action.SecurityBoard, action.SecurityCode)
	if err != nil {
		return trengin.Position{}, nil, fmt.Errorf("get security: %w", err)
	}

	openPrice, commission, err := f.openMarketOrder(ctx, security, action.Type, action.Quantity)
	if err != nil {
		return trengin.Position{}, nil, err
	}

	position, err := trengin.NewPosition(action, time.Now(), openPrice)
	if err != nil {
		return trengin.Position{}, nil, fmt.Errorf("new position: %w", err)
	}
	position.AddCommission(commission)

	var stopLossID, takeProfitID int32
	if action.StopLossOffset != 0 {
		stopLoss := openPrice - action.StopLossOffset*action.Type.Multiplier()
		stopLossID, err = f.setStopLoss(security, stopLoss, *position)
		if err != nil {
			return trengin.Position{}, nil, fmt.Errorf("set stop loss: %w", err)
		}
	}
	if action.TakeProfitOffset != 0 {
		takeProfit := openPrice + action.TakeProfitOffset*action.Type.Multiplier()
		takeProfitID, err = f.setTakeProfit(security, takeProfit, *position)
		if err != nil {
			return trengin.Position{}, nil, fmt.Errorf("set take profit: %w", err)
		}
	}

	positionClosed := make(chan trengin.Position, 1)
	fnmPosition := newFinamPosition(position, security, stopLossID, takeProfitID, positionClosed)
	f.positionStorage.Store(fnmPosition)

	return *position, positionClosed, nil
}

func (f *Finam) ChangeConditionalOrder(
	_ context.Context,
	action trengin.ChangeConditionalOrderAction,
) (trengin.Position, error) {
	fnmPosition, unlockPosition, err := f.positionStorage.Load(action.PositionID)
	if err != nil {
		return trengin.Position{}, fmt.Errorf("load position: %w", err)
	}
	defer unlockPosition()

	if action.StopLoss != 0 {
		if _, err := f.client.CancelStop(fnmPosition.StopLossID()); err != nil {
			return trengin.Position{}, fmt.Errorf("cancel stop: %w", err)
		}

		stopLossID, err := f.setStopLoss(fnmPosition.Security(), action.StopLoss, fnmPosition.Position())
		if err != nil {
			return trengin.Position{}, fmt.Errorf("set stop loss: %w", err)
		}
		fnmPosition.SetStopLoss(stopLossID, action.StopLoss)
	}

	if action.TakeProfit != 0 {
		if _, err := f.client.CancelStop(fnmPosition.TakeProfitID()); err != nil {
			return trengin.Position{}, fmt.Errorf("cancel stop: %w", err)
		}

		takeProfitID, err := f.setTakeProfit(fnmPosition.Security(), action.TakeProfit, fnmPosition.Position())
		if err != nil {
			return trengin.Position{}, fmt.Errorf("set take profit: %w", err)
		}
		fnmPosition.SetTakeProfitID(takeProfitID, action.TakeProfit)
	}
	return fnmPosition.Position(), nil

}

func (f *Finam) ClosePosition(
	ctx context.Context,
	action trengin.ClosePositionAction,
) (trengin.Position, error) {
	fnmPosition, unlockPosition, err := f.positionStorage.Load(action.PositionID)
	if err != nil {
		return trengin.Position{}, fmt.Errorf("load position: %w", err)
	}
	defer unlockPosition()

	if err := f.cancelStopOrders(fnmPosition); err != nil {
		return trengin.Position{}, fmt.Errorf("cancel stop orders: %w", err)
	}

	security := fnmPosition.Security()
	position := fnmPosition.Position()
	closePrice, commission, err := f.openMarketOrder(ctx, security, position.Type.Inverse(), position.Quantity)
	if err != nil {
		return trengin.Position{}, fmt.Errorf("open market order: %w", err)
	}

	position.AddCommission(commission)
	if err := fnmPosition.Close(closePrice); err != nil {
		return trengin.Position{}, fmt.Errorf("close: %w", err)
	}

	f.logger.Info("Position was closed", zap.Any("fnmPosition", fnmPosition))
	return fnmPosition.Position(), nil
}

func (f *Finam) trackOpenPosition(ctx context.Context) error {
	orders, trades, unsubscribe := f.orderTradeListener.Subscribe()
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-orders:
			continue
		case trade := <-trades:
			if err := f.processTrade(ctx, trade); err != nil {
				return fmt.Errorf("process trade: %w", err)
			}
		}
	}
}

func (f *Finam) processTrade(ctx context.Context, trade *tradeapi.TradeEvent) error {
	return f.positionStorage.ForEach(func(fnmPosition *finamPosition) error {
		position := fnmPosition.Position()
		if trade.SecurityCode != position.SecurityCode { //todo что делать с board?
			return nil
		}
		longClosed := position.IsLong() && trade.GetBuySell() == tradeapi.BuySell_BUY_SELL_SELL
		shortClosed := position.IsShort() && trade.GetBuySell() == tradeapi.BuySell_BUY_SELL_BUY
		if !longClosed && !shortClosed {
			return nil
		}

		stopOrderExecuted, err := f.stopOrderExecuted(ctx, fnmPosition)
		if err != nil {
			return fmt.Errorf("conditional orders found: %w", err)
		}
		if !stopOrderExecuted {
			return nil
		}

		logger := f.logger.With(zap.Any("position", position))
		fnmPosition.AddOrderTrade(trade)

		var executedQuantity int64
		for _, trade := range fnmPosition.Trades() {
			executedQuantity += trade.GetQuantity() / int64(fnmPosition.Security().LotSize)
		}
		if executedQuantity < position.Quantity {
			logger.Info("Position partially closed", zap.Any("executedQuantity", executedQuantity))
			return nil
		}

		if err := f.cancelStopOrders(fnmPosition); err != nil {
			return fmt.Errorf("cancel stop orders: %w", err)
		}

		fnmPosition.AddCommission(trade.Commission)
		if err := fnmPosition.Close(trade.Price); err != nil {
			if errors.Is(err, trengin.ErrAlreadyClosed) {
				logger.Info("Position already closed")
				return nil
			}
			return fmt.Errorf("close: %w", err)
		}
		logger.Info("Position was closed by order trades", zap.Any("trades", fnmPosition.Trades()))
		return nil
	})
}

func (f *Finam) stopOrderExecuted(ctx context.Context, position *finamPosition) (bool, error) {
	result, err := f.client.GetStops(true, false, false)
	if err != nil {
		return false, fmt.Errorf("get stops: %w", err)
	}

	for _, stop := range result.GetStops() {
		if stop.StopId == position.stopLossID || stop.StopId == position.takeProfitID {
			return true, nil
		}
	}
	return false, nil
}

// Return openPrice, commission
func (f *Finam) openMarketOrder(
	ctx context.Context,
	security *tradeapi.Security,
	positionType trengin.PositionType,
	quantity int64,
) (float64, float64, error) {
	orders, trades, unsubscribe := f.orderTradeListener.Subscribe()
	defer unsubscribe()

	req := &tradeapi.NewOrderRequest{
		ClientId:      f.clientID,
		SecurityBoard: security.Board,
		SecurityCode:  security.Code,
		BuySell:       f.buySell(positionType),
		Quantity:      int32(quantity),
		UseCredit:     f.useCredit,
		Property:      tradeapi.OrderProperty_ORDER_PROPERTY_PUT_IN_QUEUE,
	}
	orderResult, err := f.client.NewOrder(req)
	if err != nil {
		return 0, 0, fmt.Errorf("new order: %w", err)
	}

	trade, err := f.waitTrade(ctx, orderResult.TransactionId, trades, orders)
	if err != nil {
		return 0, 0, fmt.Errorf("wait trade: %w", err)
	}
	f.logger.Debug("Market order executed", zap.Any("trade", trade))
	// todo привести комиссию в рублях в доллары
	return trade.Price, trade.Commission, nil
}

func (f *Finam) waitTrade(
	ctx context.Context,
	transactionID int32,
	trades <-chan *tradeapi.TradeEvent,
	orders <-chan *tradeapi.OrderEvent,
) (*tradeapi.TradeEvent, error) {
	var orderNo int64
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(15 * time.Second):
			return nil, errors.New("trade wait timeout")
		case o := <-orders:
			if orderNo != 0 {
				continue
			}
			if o.TransactionId != transactionID {
				continue
			}
			if o.OrderNo == 0 {
				continue
			}
			orderNo = o.OrderNo
		case trade := <-trades:
			if orderNo == 0 {
				continue
			}
			if trade.OrderNo == orderNo {
				return trade, nil
			}
		}
	}
}

func (f *Finam) buySell(positionType trengin.PositionType) tradeapi.BuySell {
	if positionType.IsShort() {
		return tradeapi.BuySell_BUY_SELL_SELL
	}
	return tradeapi.BuySell_BUY_SELL_BUY
}

func (f *Finam) setStopLoss(
	security *tradeapi.Security,
	stopLoss float64,
	position trengin.Position,
) (int32, error) {
	protectiveSpread := f.addProtectiveSpread(position.Type, stopLoss)
	stopResult, err := f.client.NewStop(&tradeapi.NewStopRequest{
		ClientId:      f.clientID,
		SecurityBoard: security.Board,
		SecurityCode:  security.Code,
		BuySell:       f.buySell(position.Type.Inverse()),
		StopLoss: &tradeapi.StopLoss{
			ActivationPrice: f.round(stopLoss, security.Decimals),
			Price:           f.round(protectiveSpread, security.Decimals),
			Quantity: &tradeapi.StopQuantity{
				Value: float64(position.Quantity),
				Units: tradeapi.StopQuantityUnits_STOP_QUANTITY_UNITS_LOTS,
			},
			UseCredit: f.useCredit,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("new stop: %w", err)
	}

	return stopResult.StopId, nil
}
func (f *Finam) setTakeProfit(
	security *tradeapi.Security,
	takeProfit float64,
	position trengin.Position,
) (int32, error) {
	stopResult, err := f.client.NewStop(&tradeapi.NewStopRequest{
		ClientId:      f.clientID,
		SecurityBoard: security.Board,
		SecurityCode:  security.Code,
		BuySell:       f.buySell(position.Type.Inverse()),
		TakeProfit: &tradeapi.TakeProfit{
			ActivationPrice: f.round(takeProfit, security.Decimals),
			SpreadPrice: &tradeapi.StopPrice{
				Value: f.protectiveSpreadPercent,
				Units: tradeapi.StopPriceUnits_STOP_PRICE_UNITS_PERCENT,
			},
			Quantity: &tradeapi.StopQuantity{
				Value: float64(position.Quantity),
				Units: tradeapi.StopQuantityUnits_STOP_QUANTITY_UNITS_LOTS,
			},
			UseCredit: f.useCredit,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("new stop: %w", err)
	}

	return stopResult.StopId, nil
}

func (f *Finam) addProtectiveSpread(positionType trengin.PositionType, price float64) float64 {
	protectiveSpread := price * f.protectiveSpreadPercent / 100
	return price - positionType.Multiplier()*protectiveSpread
}

func (f *Finam) round(val float64, decimals int32) float64 {
	return math.Round(val*math.Pow10(int(decimals))) / math.Pow10(int(decimals))
}

func (f *Finam) cancelStopOrders(position *finamPosition) error {
	// todo если стоп заявка уже отменена?
	if _, err := f.client.CancelStop(position.StopLossID()); err != nil {
		return fmt.Errorf("cancel stop loss: %w", err)
	}
	if _, err := f.client.CancelStop(position.TakeProfitID()); err != nil {
		return fmt.Errorf("cancel take profit: %w", err)
	}
	return nil
}
