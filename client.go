package fnmbroker

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/assets"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/auth"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/orders"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultFinamEndpoint   = "api.finam.ru:443"
	jwtRefreshInterval     = 12 * time.Minute
	jwtRefreshRetryTimeout = 5 * time.Second
	authCallTimeout        = 30 * time.Second
)

// tokenAgent injects JWT into every gRPC call.
type tokenAgent struct {
	mu  sync.RWMutex
	jwt string
}

func (a *tokenAgent) setJWT(jwt string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.jwt = jwt
}

func (a *tokenAgent) getJWT() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.jwt
}

func (a *tokenAgent) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	jwt := a.getJWT()
	if jwt == "" {
		return map[string]string{}, nil
	}
	return map[string]string{"Authorization": jwt}, nil
}

func (a *tokenAgent) RequireTransportSecurity() bool {
	return true
}

type finamAPIClient struct {
	secret     string
	accountID  string
	endpoint   string
	logger     *zap.Logger
	tokenAgent *tokenAgent

	conn   *grpc.ClientConn
	auth   auth.AuthServiceClient
	assets assets.AssetsServiceClient
	orders orders.OrdersServiceClient
}

func newFinamAPIClient(
	ctx context.Context,
	secret, accountID, endpoint string,
	logger *zap.Logger,
) (*finamAPIClient, error) {
	if endpoint == "" {
		endpoint = defaultFinamEndpoint
	}
	tokenAgent := &tokenAgent{}

	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})),
		grpc.WithPerRPCCredentials(tokenAgent),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                4 * time.Minute,
			Timeout:             30 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial finam api: %w", err)
	}

	c := &finamAPIClient{
		secret:     secret,
		accountID:  accountID,
		endpoint:   endpoint,
		logger:     logger,
		tokenAgent: tokenAgent,
		conn:       conn,
		auth:       auth.NewAuthServiceClient(conn),
		assets:     assets.NewAssetsServiceClient(conn),
		orders:     orders.NewOrdersServiceClient(conn),
	}
	if err := c.refreshJWT(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go c.runJWTRefresher(ctx)
	return c, nil
}

func (c *finamAPIClient) close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *finamAPIClient) refreshJWT(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, authCallTimeout)
	defer cancel()

	resp, err := c.auth.Auth(ctx, &auth.AuthRequest{Secret: c.secret})
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	c.tokenAgent.setJWT(resp.GetToken())
	return nil
}

func (c *finamAPIClient) runJWTRefresher(ctx context.Context) {
	ticker := time.NewTicker(jwtRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.refreshJWT(ctx); err != nil {
				c.logger.Error("Failed to refresh JWT", zap.Error(err))
				ticker.Reset(jwtRefreshRetryTimeout)
				continue
			}
			ticker.Reset(jwtRefreshInterval)
		}
	}
}
