package fnmbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/assets"
	"go.uber.org/zap"
)

// Common trading board -> MIC hints for symbol construction (ticker@mic).
var boardToMic = map[string]string{
	"TQBR":   "MISX",
	"TQTF":   "MISX",
	"TQPI":   "MISX",
	"TQOB":   "MISX",
	"TQCB":   "MISX",
	"TQIR":   "MISX",
	"MTQR":   "MISX",
	"SPBFUT": "RTSX",
	"FUT":    "RTSX",
	"FUTS":   "RTSX",
}

type securityProvider struct {
	client    *finamAPIClient
	cacheFile string
	logger    *zap.Logger

	// key: board:code (e.g. TQBR:SBER) or mic:code (e.g. MISX:SBER)
	securities map[string]*security
	assets     []*assets.Asset
}

func newSecurityProvider(
	client *finamAPIClient,
	cacheFile string,
	logger *zap.Logger,
) (*securityProvider, error) {
	s := &securityProvider{
		client:     client,
		cacheFile:  cacheFile,
		logger:     logger,
		securities: make(map[string]*security),
	}

	cached, err := s.readFromCache()
	if err == nil {
		for _, sec := range cached {
			s.store(sec)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read from cache: %w", err)
	}
	return s, nil
}

func (s *securityProvider) Get(ctx context.Context, board, code string) (*security, error) {
	if sec, ok := s.securities[s.key(board, code)]; ok {
		return sec, nil
	}

	sec, err := s.resolve(ctx, board, code)
	if err != nil {
		return nil, err
	}
	s.store(sec)
	if err := s.writeCache(); err != nil {
		s.logger.Warn("Failed to write securities cache", zap.Error(err))
	}
	return sec, nil
}

func (s *securityProvider) store(sec *security) {
	s.securities[s.key(sec.Board, sec.Code)] = sec
	if sec.Mic != "" {
		s.securities[s.key(sec.Mic, sec.Code)] = sec
	}
}

func (s *securityProvider) key(board, code string) string {
	return fmt.Sprintf("%s:%s", board, code)
}

func (s *securityProvider) loadAssets(ctx context.Context) error {
	var (
		cursor int64
		list   []*assets.Asset
	)
	for {
		resp, err := s.client.assets.AllAssets(ctx, &assets.AllAssetsRequest{
			Cursor:     cursor,
			OnlyActive: true,
		})
		if err != nil {
			return fmt.Errorf("all assets: %w", err)
		}
		list = append(list, resp.GetAssets()...)
		next := resp.GetNextCursor()
		if next == 0 || next == cursor {
			break
		}
		cursor = next
	}
	s.assets = list
	return nil
}

func (s *securityProvider) resolve(ctx context.Context, board, code string) (*security, error) {
	if len(s.assets) == 0 {
		if err := s.loadAssets(ctx); err != nil {
			s.logger.Debug("Assets list unavailable, resolve by symbol hints", zap.Error(err))
		}
	}

	candidates := s.candidateSymbols(board, code)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("security not found: %s:%s", board, code)
	}

	var lastErr error
	for _, symbol := range candidates {
		resp, err := s.client.assets.GetAsset(ctx, &assets.GetAssetRequest{
			Symbol:    symbol,
			AccountId: s.client.accountID,
		})
		if err != nil {
			lastErr = err
			continue
		}

		lotSize := int64(mustDecimalToFloat(resp.GetLotSize()))
		if lotSize <= 0 {
			lotSize = 1
		}
		sec := &security{
			Symbol:   symbol,
			Board:    resp.GetBoard(),
			Code:     resp.GetTicker(),
			Mic:      resp.GetMic(),
			Decimals: resp.GetDecimals(),
			LotSize:  lotSize,
		}
		if sec.Code == "" {
			sec.Code = code
		}
		if sec.Board == "" {
			sec.Board = board
		}

		if strings.EqualFold(sec.Board, board) || strings.EqualFold(sec.Mic, board) {
			return sec, nil
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("resolve security %s:%s: %w", board, code, lastErr)
	}
	return nil, fmt.Errorf("security not found: %s:%s", board, code)
}

func (s *securityProvider) candidateSymbols(board, code string) []string {
	seen := make(map[string]struct{})
	var symbols []string
	add := func(symbol string) {
		if symbol == "" {
			return
		}
		if _, ok := seen[symbol]; ok {
			return
		}
		seen[symbol] = struct{}{}
		symbols = append(symbols, symbol)
	}

	board = strings.TrimSpace(board)
	code = strings.TrimSpace(code)
	if board != "" {
		add(fmt.Sprintf("%s@%s", code, board))
	}
	if mic, ok := boardToMic[strings.ToUpper(board)]; ok {
		add(fmt.Sprintf("%s@%s", code, mic))
	}
	for _, asset := range s.assets {
		if !strings.EqualFold(asset.GetTicker(), code) {
			continue
		}
		add(asset.GetSymbol())
	}
	return symbols
}

func (s *securityProvider) readFromCache() ([]*security, error) {
	content, err := os.ReadFile(s.cacheFile)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var securities []*security
	if err := json.Unmarshal(content, &securities); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return securities, nil
}

func (s *securityProvider) writeCache() error {
	dir := filepath.Dir(s.cacheFile)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	unique := make([]*security, 0, len(s.securities))
	seen := make(map[string]struct{})
	for _, sec := range s.securities {
		if _, ok := seen[sec.Symbol]; ok {
			continue
		}
		seen[sec.Symbol] = struct{}{}
		unique = append(unique, sec)
	}

	content, err := json.Marshal(unique)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(s.cacheFile, content, 0o600); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}
