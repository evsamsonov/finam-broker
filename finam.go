// Package fnmbroker implements [trengin.Broker] using [Finam Trade API].
//
// [Finam Trade API]: https://finamweb.github.io/trade-api-docs/
package fnmbroker

import (
	"context"
	"errors"
	"fmt"
	"time"

	finamclient "github.com/evsamsonov/FinamTradeGo/v2"
	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
	"github.com/evsamsonov/trengin/v2"
	"go.uber.org/zap"

	"github.com/evsamsonov/finam-broker/internal/fnmposition"
	"github.com/evsamsonov/finam-broker/internal/tradevent"
)

var _ trengin.Broker = &Finam{}

const (
	defaultProtectiveSpreadPercent = 1
)

type Finam struct {
	clientID                string
	token                   string
	protectiveSpreadPercent float64
	logger                  *zap.Logger

	client             finamclient.IFinamClient
	positionStorage    *fnmposition.Storage
	orderTradeListener *tradevent.OrderTradeListener
}

type Option func(*Finam)

// WithLogger returns Option which sets logger. The default logger is no-op Logger
func WithLogger(logger *zap.Logger) Option {
	return func(t *Finam) {
		t.logger = logger
	}
}

// WithProtectiveSpreadPercent returns Option which sets protective spread
// in percent for executing orders. The default value is 1%
func WithProtectiveSpreadPercent(protectiveSpread float64) Option {
	return func(f *Finam) {
		f.protectiveSpreadPercent = protectiveSpread
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
		positionStorage:         fnmposition.NewStorage(),
		protectiveSpreadPercent: defaultProtectiveSpreadPercent,
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
	f.orderTradeListener = tradevent.NewOrderTradeListener(
		finamClient,
		f.clientID,
		f.logger,
	)

	<-ctx.Done()
	return ctx.Err()
}

// OpenPosition
// see https://finamweb.github.io/trade-api-docs/grpc/orders
func (f *Finam) OpenPosition(ctx context.Context, action trengin.OpenPositionAction) (trengin.Position, trengin.PositionClosed, error) {
	security := fnmposition.Security{
		Board: action.SecurityBoard,
		Code:  action.SecurityCode,
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
	f.positionStorage.Store(
		fnmposition.NewPosition(position, security, stopLossID, takeProfitID, positionClosed),
	)

	return *position, positionClosed, nil

}

func (f *Finam) ClosePosition(ctx context.Context, action trengin.ClosePositionAction) (trengin.Position, error) {
	//TODO implement me
	panic("implement me")
}

func (f *Finam) ChangeConditionalOrder(ctx context.Context, action trengin.ChangeConditionalOrderAction) (trengin.Position, error) {
	//TODO implement me
	panic("implement me")
}

// Return openPrice, commission
func (f *Finam) openMarketOrder(
	ctx context.Context,
	security fnmposition.Security,
	positionType trengin.PositionType,
	quantity int64,
) (float64, float64, error) {
	orders, trades, unsubscribe := f.orderTradeListener.Subscribe()
	defer unsubscribe()

	orderResult, err := f.client.NewOrder(&tradeapi.NewOrderRequest{
		ClientId:      f.clientID,
		SecurityBoard: security.Board,
		SecurityCode:  security.Code,
		BuySell:       f.buySell(positionType),
		Quantity:      int32(quantity),
		UseCredit:     false,
		Property:      tradeapi.OrderProperty_ORDER_PROPERTY_PUT_IN_QUEUE,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("new order: %w", err)
	}

	trade, err := f.waitTrade(ctx, orderResult.TransactionId, trades, orders)
	if err != nil {
		return 0, 0, fmt.Errorf("wait trade: %w", err)
	}
	// todo комиссия в рублях??
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
		case <-time.After(5 * time.Second):
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

func (f *Finam) setStopLoss(security fnmposition.Security, stopLoss float64, position trengin.Position) (int32, error) {
	stopResult, err := f.client.NewStop(&tradeapi.NewStopRequest{
		ClientId:      f.clientID,
		SecurityBoard: security.Board,
		SecurityCode:  security.Code,
		BuySell:       f.buySell(position.Type.Inverse()),
		StopLoss: &tradeapi.StopLoss{
			ActivationPrice: stopLoss,
			Price:           f.addProtectiveSpread(position.Type, stopLoss),
			Quantity: &tradeapi.StopQuantity{
				Value: float64(position.Quantity),
				Units: tradeapi.StopQuantityUnits_STOP_QUANTITY_UNITS_LOTS,
			},
			UseCredit: false, // on?
		},
	})
	if err != nil {
		return 0, fmt.Errorf("new stop: %w", err)
	}

	return stopResult.StopId, nil
}
func (f *Finam) setTakeProfit(security fnmposition.Security, takeProfit float64, position trengin.Position) (int32, error) {
	stopResult, err := f.client.NewStop(&tradeapi.NewStopRequest{
		ClientId:      f.clientID,
		SecurityBoard: security.Board,
		SecurityCode:  security.Code,
		BuySell:       f.buySell(position.Type.Inverse()),
		TakeProfit: &tradeapi.TakeProfit{
			ActivationPrice: takeProfit,
			MarketPrice:     true,
			Quantity: &tradeapi.StopQuantity{
				Value: float64(position.Quantity),
				Units: tradeapi.StopQuantityUnits_STOP_QUANTITY_UNITS_LOTS,
			},
			UseCredit: false, // todo on?
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

//{"order":{"order_no":"24278343703","transaction_id":40725538,"security_code":"SNAP","client_id":"757953RDW8I","status":"ORDER_STATUS_MATCHED","buy_sell":"BUY_SELL_BUY","created_at":{"seconds":"1703184643","nanos":0},"price":0,"quantity":1,"balance":0,"message":"","currency":"USD","condition":{"type":"ORDER_CONDITION_TYPE_UNSPECIFIED","price":0,"time":null},"valid_before":{"type":"ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION","time":null},"accepted_at":null}}
//21:50:44
//{"trade":{"security_code":"SNAP","trade_no":"24278449122","order_no":"24278343703","client_id":"757953RDW8I","created_at":{"seconds":"1703184643","nanos":0},"quantity":"1","price":17.15,"value":17.15,"buy_sell":"BUY_SELL_BUY","commission":0,"currency":"USD","accrued_interest":0}}
//21:50:44
//{"order":{"order_no":"24278343703","transaction_id":40725538,"security_code":"SNAP","client_id":"757953RDW8I","status":"ORDER_STATUS_ACTIVE","buy_sell":"BUY_SELL_BUY","created_at":{"seconds":"1703184643","nanos":0},"price":0,"quantity":1,"balance":1,"message":"","currency":"USD","condition":{"type":"ORDER_CONDITION_TYPE_UNSPECIFIED","price":0,"time":null},"valid_before":{"type":"ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION","time":null},"accepted_at":null}}
//21:50:44
//{"order":{"order_no":"0","transaction_id":40725538,"security_code":"SNAP","client_id":"757953RDW8I","status":"ORDER_STATUS_ACTIVE","buy_sell":"BUY_SELL_BUY","created_at":null,"price":0,"quantity":1,"balance":1,"message":"","currency":"USD","condition":{"type":"ORDER_CONDITION_TYPE_UNSPECIFIED","price":0,"time":null},"valid_before":{"type":"ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION","time":null},"accepted_at":{"seconds":"1703184643","nanos":0}}}
//21:50:44
//{"order":{"order_no":"0","transaction_id":40725538,"security_code":"SNAP","client_id":"757953RDW8I","status":"ORDER_STATUS_NONE","buy_sell":"BUY_SELL_BUY","created_at":null,"price":0,"quantity":1,"balance":1,"message":"","currency":"USD","condition":{"type":"ORDER_CONDITION_TYPE_UNSPECIFIED","price":0,"time":null},"valid_before":{"type":"ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION","time":null},"accepted_at":{"seconds":"1703184643","nanos":0}}}
//21:50:44
//{"response":{"errors":[],"request_id":"1","success":true}}
//21:50:34
//Received response from trade-api.finam.ru
//21:50:34
//{"order_trade_subscribe_request":{"client_ids":["757953RDW8I"],"include_orders":true,"include_trades":true,"request_id":"1"}}
//21:50:34
//Sent request to trade-api.finam.ru
//21:50:30
//Online
