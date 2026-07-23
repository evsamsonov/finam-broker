// Package fnmbroker implements [trengin.Broker] using [Finam Trade API].
//
// [Finam Trade API]: https://tradeapi.finam.ru/docs/guides/grpc/
// [trengin.Broker]: https://github.com/evsamsonov/trengin
package fnmbroker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	tradeapi "github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"github.com/evsamsonov/trengin/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ trengin.Broker = &Finam{}

const (
	defaultProtectiveSpreadPercent = 1
	defaultSecurityCacheFile       = "securities.json"
	tradeWaitTimeout               = 15 * time.Second
)

type Finam struct {
	accountID               string
	token                   string
	endpoint                string
	logger                  *zap.Logger
	protectiveSpreadPercent float64
	securityCacheFile       string

	client             *finamAPIClient
	positionStorage    *positionStorage
	orderTradeListener *orderTradeListener
	securityProvider   *securityProvider
}

type Option func(*Finam)

// WithLogger returns Option which sets logger. The default logger is no-op Logger.
func WithLogger(logger *zap.Logger) Option {
	return func(f *Finam) {
		f.logger = logger
	}
}

// WithProtectiveSpreadPercent returns Option which sets protective spread
// in percent for executing orders. The default value is 1%.
func WithProtectiveSpreadPercent(protectiveSpread float64) Option {
	return func(f *Finam) {
		f.protectiveSpreadPercent = protectiveSpread
	}
}

// WithUseCredit is deprecated: Trade API v1 does not expose a use-credit flag.
// The option is kept for backward compatibility and has no effect.
func WithUseCredit(_ bool) Option {
	return func(*Finam) {}
}

// WithSecurityCacheFile returns Option which sets path to securities cache file.
// The default value is securities.json in current directory.
func WithSecurityCacheFile(securityCacheFile string) Option {
	return func(f *Finam) {
		f.securityCacheFile = securityCacheFile
	}
}

// WithEndpoint returns Option which sets Finam Trade API gRPC endpoint.
// The default value is api.finam.ru:443.
func WithEndpoint(endpoint string) Option {
	return func(f *Finam) {
		f.endpoint = endpoint
	}
}

// New creates a new Finam object. It takes API secret token and account id.
//
// [API tokens]: https://tradeapi.finam.ru/docs/tokens/
func New(token, accountID string, opts ...Option) *Finam {
	finam := &Finam{
		accountID:               accountID,
		token:                   token,
		logger:                  zap.NewNop(),
		positionStorage:         newPositionStorage(),
		protectiveSpreadPercent: defaultProtectiveSpreadPercent,
		securityCacheFile:       defaultSecurityCacheFile,
	}
	for _, opt := range opts {
		opt(finam)
	}
	return finam
}

// Run initializes required objects and starts to track open positions.
func (f *Finam) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	f.client, err = newFinamAPIClient(ctx, f.token, f.accountID, f.endpoint, f.logger)
	if err != nil {
		return fmt.Errorf("new finam client: %w", err)
	}
	defer func() {
		if closeErr := f.client.close(); closeErr != nil {
			f.logger.Warn("Failed to close finam client", zap.Error(closeErr))
		}
	}()

	f.securityProvider, err = newSecurityProvider(f.client, f.securityCacheFile, f.logger)
	if err != nil {
		return fmt.Errorf("get securities: %w", err)
	}

	f.orderTradeListener = newOrderTradeListener(f.client, f.logger)

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

// OpenPosition opens a market position and optional SL/TP orders.
// See https://tradeapi.finam.ru/docs/guides/grpc/
func (f *Finam) OpenPosition(
	ctx context.Context,
	action trengin.OpenPositionAction,
) (trengin.Position, trengin.PositionClosed, error) {
	sec, err := f.securityProvider.Get(ctx, action.SecurityBoard, action.SecurityCode)
	if err != nil {
		return trengin.Position{}, nil, fmt.Errorf("get security: %w", err)
	}

	openPrice, err := f.openMarketOrder(ctx, sec, action.Type, action.Quantity)
	if err != nil {
		return trengin.Position{}, nil, err
	}

	position, err := trengin.NewPosition(action, time.Now(), openPrice)
	if err != nil {
		return trengin.Position{}, nil, fmt.Errorf("new position: %w", err)
	}

	var stopLoss, takeProfit float64
	if action.StopLossOffset != 0 {
		stopLoss = openPrice - action.StopLossOffset*action.Type.Multiplier()
	}
	if action.TakeProfitOffset != 0 {
		takeProfit = openPrice + action.TakeProfitOffset*action.Type.Multiplier()
	}
	stopID, err := f.setStop(ctx, sec, stopLoss, takeProfit, *position)
	if err != nil {
		return trengin.Position{}, nil, fmt.Errorf("set stop: %w", err)
	}

	positionClosed := make(chan trengin.Position, 1)
	fnmPosition := newFinamPosition(position, sec, stopID, positionClosed)
	f.positionStorage.Store(fnmPosition)

	return *position, positionClosed, nil
}

func (f *Finam) ChangeConditionalOrder(
	ctx context.Context,
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

	if fnmPosition.StopID() != "" {
		if err := f.cancelStop(ctx, fnmPosition.StopID()); err != nil {
			return trengin.Position{}, fmt.Errorf("cancel stop: %w", err)
		}
	}

	stopID, err := f.setStop(
		ctx,
		fnmPosition.Security(),
		action.StopLoss,
		action.TakeProfit,
		fnmPosition.Position(),
	)
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

	if fnmPosition.StopID() != "" {
		if err := f.cancelStop(ctx, fnmPosition.StopID()); err != nil {
			return trengin.Position{}, fmt.Errorf("cancel stop: %w", err)
		}
	}

	sec := fnmPosition.Security()
	position := fnmPosition.Position()
	closePrice, err := f.openMarketOrder(ctx, sec, position.Type.Inverse(), position.Quantity)
	if err != nil {
		return trengin.Position{}, fmt.Errorf("open market order: %w", err)
	}

	if err := fnmPosition.Close(closePrice); err != nil {
		return trengin.Position{}, fmt.Errorf("close: %w", err)
	}

	f.logger.Info("Position was closed", zap.Any("fnmPosition", fnmPosition))
	return fnmPosition.Position(), nil
}

func (f *Finam) trackOpenPosition(ctx context.Context) error {
	ordersCh, trades, unsubscribe := f.orderTradeListener.Subscribe()
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ordersCh:
			continue
		case trade := <-trades:
			if err := f.processTrade(ctx, trade); err != nil {
				return fmt.Errorf("process trade: %w", err)
			}
		}
	}
}

func (f *Finam) processTrade(ctx context.Context, trade *tradeapi.AccountTrade) error {
	return f.positionStorage.ForEach(func(fnmPosition *finamPosition) error {
		position := fnmPosition.Position()
		sec := fnmPosition.Security()
		if !sameInstrument(trade.GetSymbol(), sec) {
			return nil
		}
		longClosed := position.IsLong() && trade.GetSide() == tradeapi.Side_SIDE_SELL
		shortClosed := position.IsShort() && trade.GetSide() == tradeapi.Side_SIDE_BUY
		if !longClosed && !shortClosed {
			return nil
		}

		stopOrderExecuted, err := f.stopOrderExecuted(ctx, fnmPosition)
		if err != nil {
			return fmt.Errorf("stop order executed: %w", err)
		}
		if !stopOrderExecuted {
			return nil
		}

		logger := f.logger.With(zap.Any("position", position))
		fnmPosition.AddOrderTrade(trade)

		var executedQuantity int64
		for _, t := range fnmPosition.Trades() {
			executedQuantity += sec.lots(mustDecimalToFloat(t.GetSize()))
		}
		if executedQuantity < position.Quantity {
			logger.Info("Position partially closed", zap.Any("executedQuantity", executedQuantity))
			return nil
		}

		closePrice := mustDecimalToFloat(trade.GetPrice())
		if err := fnmPosition.Close(closePrice); err != nil {
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
	stopID := position.StopID()
	if stopID == "" {
		return false, nil
	}

	state, err := f.client.orders.GetOrder(ctx, &orders.GetOrderRequest{
		AccountId: f.accountID,
		OrderId:   stopID,
	})
	if err != nil {
		return false, fmt.Errorf("get order: %w", err)
	}

	switch state.GetStatus() {
	case orders.OrderStatus_ORDER_STATUS_SL_EXECUTED,
		orders.OrderStatus_ORDER_STATUS_TP_EXECUTED,
		orders.OrderStatus_ORDER_STATUS_FILLED,
		orders.OrderStatus_ORDER_STATUS_EXECUTED:
		return true, nil
	default:
		return false, nil
	}
}

func (f *Finam) openMarketOrder(
	ctx context.Context,
	sec *security,
	positionType trengin.PositionType,
	quantity int64,
) (float64, error) {
	_, trades, unsubscribe := f.orderTradeListener.Subscribe()
	defer unsubscribe()

	order := &orders.Order{
		AccountId:   f.accountID,
		Symbol:      sec.Symbol,
		Quantity:    intToDecimal(sec.pieces(quantity)),
		Side:        f.side(positionType),
		Type:        orders.OrderType_ORDER_TYPE_MARKET,
		TimeInForce: orders.TimeInForce_TIME_IN_FORCE_DAY,
	}
	orderState, err := f.client.orders.PlaceOrder(ctx, order)
	if err != nil {
		return 0, fmt.Errorf("place order: %w", err)
	}
	f.logger.Debug("Order created", zap.String("orderId", orderState.GetOrderId()))

	trade, err := f.waitTrade(ctx, orderState.GetOrderId(), trades)
	if err != nil {
		return 0, fmt.Errorf("wait trade: %w", err)
	}
	f.logger.Debug("Market order executed", zap.Any("trade", trade))
	return mustDecimalToFloat(trade.GetPrice()), nil
}

func (f *Finam) waitTrade(
	ctx context.Context,
	orderID string,
	trades <-chan *tradeapi.AccountTrade,
) (*tradeapi.AccountTrade, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(tradeWaitTimeout):
			return nil, errors.New("trade wait timeout")
		case trade := <-trades:
			if trade.GetOrderId() != orderID {
				continue
			}
			return trade, nil
		}
	}
}

func (f *Finam) side(positionType trengin.PositionType) tradeapi.Side {
	if positionType.IsShort() {
		return tradeapi.Side_SIDE_SELL
	}
	return tradeapi.Side_SIDE_BUY
}

func (f *Finam) setStop(
	ctx context.Context,
	sec *security,
	stopLossPrice float64,
	takeProfitPrice float64,
	position trengin.Position,
) (string, error) {
	if stopLossPrice == 0 && takeProfitPrice == 0 {
		return "", nil
	}

	sltp := &orders.SLTPOrder{
		AccountId:   f.accountID,
		Symbol:      sec.Symbol,
		Side:        f.side(position.Type.Inverse()),
		ValidBefore: orders.ValidBefore_VALID_BEFORE_GOOD_TILL_CANCEL,
	}

	pieces := intToDecimal(sec.pieces(position.Quantity))
	if stopLossPrice != 0 {
		orderPrice := f.addProtectiveSpread(position.Type, stopLossPrice)
		sltp.QuantitySl = pieces
		sltp.SlPrice = floatToDecimal(f.round(stopLossPrice, sec.Decimals))
		sltp.LimitPrice = floatToDecimal(f.round(orderPrice, sec.Decimals))
	}
	if takeProfitPrice != 0 {
		sltp.QuantityTp = pieces
		sltp.TpPrice = floatToDecimal(f.round(takeProfitPrice, sec.Decimals))
		sltp.TpGuardSpread = floatToDecimal(f.protectiveSpreadPercent)
		sltp.TpSpreadMeasure = orders.TPSpreadMeasure_TP_SPREAD_MEASURE_PERCENT
	}

	stopResult, err := f.client.orders.PlaceSLTPOrder(ctx, sltp)
	if err != nil {
		return "", fmt.Errorf("place sltp order: %w", err)
	}
	return stopResult.GetOrderId(), nil
}

func (f *Finam) cancelStop(ctx context.Context, stopID string) error {
	_, err := f.client.orders.CancelOrder(ctx, &orders.CancelOrderRequest{
		AccountId: f.accountID,
		OrderId:   stopID,
	})
	if err != nil {
		return fmt.Errorf("cancel order: %w", err)
	}
	return nil
}

func (f *Finam) addProtectiveSpread(positionType trengin.PositionType, price float64) float64 {
	protectiveSpread := price * f.protectiveSpreadPercent / 100
	return price - positionType.Multiplier()*protectiveSpread
}

func (f *Finam) round(val float64, decimals int32) float64 {
	return math.Round(val*math.Pow10(int(decimals))) / math.Pow10(int(decimals))
}

func sameInstrument(tradeSymbol string, sec *security) bool {
	if tradeSymbol == "" || sec == nil {
		return false
	}
	if tradeSymbol == sec.Symbol {
		return true
	}
	ticker, _, _ := strings.Cut(tradeSymbol, "@")
	return strings.EqualFold(ticker, sec.Code)
}
