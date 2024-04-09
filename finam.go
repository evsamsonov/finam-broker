// Package fnmbroker implements [trengin.Broker] using [Finam Trade API].
//
// [Finam Trade API]: https://finamweb.github.io/trade-api-docs/
// [trengin.Broker]: https://github.com/evsamsonov/trengin
package fnmbroker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	finamclient "github.com/evsamsonov/FinamTradeGo/v2"
	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
	"github.com/evsamsonov/trengin/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ trengin.Broker = &Finam{}

const (
	defaultProtectiveSpreadPercent = 1
	defaultUseCredit               = true
	defaultSecurityCacheFile       = "securities.json"
)

type Finam struct {
	clientID                string
	token                   string
	logger                  *zap.Logger
	protectiveSpreadPercent float64
	useCredit               bool
	securityCacheFile       string

	client             finamclient.IFinamClient
	positionStorage    *positionStorage
	orderTradeListener *orderTradeListener
	securityProvider   *securityProvider
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

// WithSecurityCacheFile returns Option which sets path to securities cache file.
// The default value is securities.json in current directory
func WithSecurityCacheFile(securityCacheFile string) Option {
	return func(f *Finam) {
		f.securityCacheFile = securityCacheFile
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
		securityCacheFile:       defaultSecurityCacheFile,
	}
	for _, opt := range opts {
		opt(finam)
	}
	return finam
}

// Run initializes required objects and starts to track an open positions
func (f *Finam) Run(ctx context.Context) error {
	var err error
	f.client, err = finamclient.NewFinamClient(f.clientID, f.token, ctx)
	if err != nil {
		return fmt.Errorf("new finam client: %w", err)
	}

	f.securityProvider, err = newSecurityProvider(f.client, f.securityCacheFile, f.logger)
	if err != nil {
		return fmt.Errorf("get securities: %w", err)
	}

	f.orderTradeListener = newOrderTradeListener(f.clientID, f.token, f.logger)

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer cancel()

		if err := f.orderTradeListener.Run(ctx); err != nil {
			return fmt.Errorf("order trade listener: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		defer cancel()

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

	var stopLoss, takeProfit float64
	if action.StopLossOffset != 0 {
		stopLoss = openPrice - action.StopLossOffset*action.Type.Multiplier()
	}
	if action.TakeProfitOffset != 0 {
		takeProfit = openPrice + action.TakeProfitOffset*action.Type.Multiplier()
	}
	stopID, err := f.setStop(security, stopLoss, takeProfit, *position)
	if err != nil {
		return trengin.Position{}, nil, fmt.Errorf("set stop: %w", err)
	}

	positionClosed := make(chan trengin.Position, 1)
	fnmPosition := newFinamPosition(position, security, stopID, positionClosed)
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

	if action.StopLoss == 0 && action.TakeProfit == 0 {
		return fnmPosition.Position(), nil
	}

	if _, err := f.client.CancelStop(fnmPosition.StopID()); err != nil {
		return trengin.Position{}, fmt.Errorf("cancel stop: %w", err)
	}

	stopID, err := f.setStop(fnmPosition.Security(), action.StopLoss, action.TakeProfit, fnmPosition.Position())
	if err != nil {
		return trengin.Position{}, err
	}
	fnmPosition.SetStop(stopID, action.StopLoss, action.TakeProfit)

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

	if _, err := f.client.CancelStop(fnmPosition.StopID()); err != nil {
		return trengin.Position{}, fmt.Errorf("cancel stop: %w", err)
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
			if err := f.processTrade(trade); err != nil {
				return fmt.Errorf("process trade: %w", err)
			}
		}
	}
}

func (f *Finam) processTrade(trade *tradeapi.TradeEvent) error {
	return f.positionStorage.ForEach(func(fnmPosition *finamPosition) error {
		position := fnmPosition.Position()
		if trade.SecurityCode != position.SecurityCode { // todo что делать с board?
			return nil
		}
		longClosed := position.IsLong() && trade.GetBuySell() == tradeapi.BuySell_BUY_SELL_SELL
		shortClosed := position.IsShort() && trade.GetBuySell() == tradeapi.BuySell_BUY_SELL_BUY
		if !longClosed && !shortClosed {
			return nil
		}

		stopOrderExecuted, err := f.stopOrderExecuted(fnmPosition)
		if err != nil {
			return fmt.Errorf("stop order executed: %w", err)
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

func (f *Finam) stopOrderExecuted(position *finamPosition) (bool, error) {
	result, err := f.client.GetStops(true, false, false)
	if err != nil {
		return false, fmt.Errorf("get stops: %w", err)
	}

	for _, stop := range result.GetStops() {
		if stop.StopId == position.stopID {
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

	// todo использовать защитный спред при открытии позиции
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
			// Find orderNo by transactionID
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
			// Find trade by orderNo
			if orderNo == 0 {
				continue
			}
			if trade.OrderNo != orderNo {
				continue
			}
			return trade, nil
		}
	}
}

func (f *Finam) buySell(positionType trengin.PositionType) tradeapi.BuySell {
	if positionType.IsShort() {
		return tradeapi.BuySell_BUY_SELL_SELL
	}
	return tradeapi.BuySell_BUY_SELL_BUY
}

func (f *Finam) setStop(
	security *tradeapi.Security,
	stopLossPrice float64,
	takeProfitPrice float64,
	position trengin.Position,
) (int32, error) {
	var stopLoss *tradeapi.StopLoss
	if stopLossPrice != 0 {
		orderPrice := f.addProtectiveSpread(position.Type, stopLossPrice)
		stopLoss = &tradeapi.StopLoss{
			ActivationPrice: f.round(stopLossPrice, security.Decimals),
			Price:           f.round(orderPrice, security.Decimals),
			Quantity: &tradeapi.StopQuantity{
				Value: float64(position.Quantity),
				Units: tradeapi.StopQuantityUnits_STOP_QUANTITY_UNITS_LOTS,
			},
			UseCredit: f.useCredit,
		}
	}

	var takeProfit *tradeapi.TakeProfit
	if takeProfitPrice != 0 {
		takeProfit = &tradeapi.TakeProfit{
			ActivationPrice: f.round(takeProfitPrice, security.Decimals),
			SpreadPrice: &tradeapi.StopPrice{
				Value: f.protectiveSpreadPercent,
				Units: tradeapi.StopPriceUnits_STOP_PRICE_UNITS_PERCENT,
			},
			Quantity: &tradeapi.StopQuantity{
				Value: float64(position.Quantity),
				Units: tradeapi.StopQuantityUnits_STOP_QUANTITY_UNITS_LOTS,
			},
			UseCredit: f.useCredit,
		}
	}

	stopResult, err := f.client.NewStop(&tradeapi.NewStopRequest{
		ClientId:      f.clientID,
		SecurityBoard: security.Board,
		SecurityCode:  security.Code,
		BuySell:       f.buySell(position.Type.Inverse()),
		StopLoss:      stopLoss,
		TakeProfit:    takeProfit,
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
