package fnmbroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	finamclient "github.com/evsamsonov/FinamTradeGo/v2"
	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
	"go.uber.org/zap"
)

type securityProvider struct {
	client    finamclient.IFinamClient
	cacheFile string
	logger    *zap.Logger

	securities map[string]*tradeapi.Security
}

func newSecurityProvider(client finamclient.IFinamClient, cacheFile string, logger *zap.Logger) (*securityProvider, error) {
	s := &securityProvider{
		client:    client,
		cacheFile: cacheFile,
		logger:    logger,
	}

	securities, err := s.getSecurities()
	if err != nil {
		return nil, fmt.Errorf("get securities: %w", err)
	}
	s.securities = make(map[string]*tradeapi.Security)
	for _, sec := range securities {
		s.securities[s.key(sec.Board, sec.Code)] = sec
	}
	return s, nil
}

func (s *securityProvider) Get(board, code string) (*tradeapi.Security, error) {
	security, ok := s.securities[s.key(board, code)]
	if !ok {
		return nil, errors.New("security not found")
	}
	return security, nil
}

func (s *securityProvider) key(board, code string) string {
	return fmt.Sprintf("%s:%s", board, code)
}

func (s *securityProvider) getSecurities() ([]*tradeapi.Security, error) {
	securities, err := s.readFromCache()
	if err == nil {
		return securities, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read from cache: %w", err)
	}

	securitiesResult, err := s.client.GetSecurities()
	if err != nil {
		return nil, fmt.Errorf("get securities: %w", err)
	}
	if err := s.writeToCache(securitiesResult.GetSecurities()); err != nil {
		return nil, fmt.Errorf("write to cache: %w", err)
	}
	return securitiesResult.GetSecurities(), nil
}

func (s *securityProvider) readFromCache() ([]*tradeapi.Security, error) {
	file, err := os.Open(s.cacheFile)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	content, err := os.ReadFile(s.cacheFile)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if err := file.Close(); err != nil {
		s.logger.Error("Failed to close file", zap.Error(err))
	}

	var securities []*tradeapi.Security
	if err := json.Unmarshal(content, &securities); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return securities, nil
}

func (s *securityProvider) writeToCache(securities []*tradeapi.Security) error {
	if err := os.MkdirAll(filepath.Dir(s.cacheFile), 0777); err != nil {
		return err
	}

	file, err := os.Create(s.cacheFile)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			s.logger.Error("Failed to close file", zap.Error(err))
		}
	}()

	content, err := json.Marshal(securities)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err = file.Write(content); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}
