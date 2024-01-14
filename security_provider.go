package fnmbroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	finamclient "github.com/evsamsonov/FinamTradeGo/v2"
	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
	"go.uber.org/zap"
)

// todo возможность указать путь к файлу
const securityProviderFile = "securities.json"

type securityProvider map[string]*tradeapi.Security

func newSecurityProvider(client finamclient.IFinamClient, logger *zap.Logger) (securityProvider, error) {
	s := make(securityProvider)

	securities, err := s.getSecurities(client, logger)
	if err != nil {
		return nil, fmt.Errorf("get securities: %w", err)
	}
	for _, sec := range securities {
		s[s.key(sec.Board, sec.Code)] = sec
	}

	return s, nil
}

func (s securityProvider) Get(board, code string) (*tradeapi.Security, error) {
	security, ok := s[s.key(board, code)]
	if !ok {
		return nil, errors.New("security not found")
	}
	return security, nil
}

func (s securityProvider) key(board, code string) string {
	return fmt.Sprintf("%s:%s", board, code)
}

func (s securityProvider) getSecurities(
	client finamclient.IFinamClient,
	logger *zap.Logger,
) ([]*tradeapi.Security, error) {
	if file, err := os.Open(securityProviderFile); !errors.Is(err, os.ErrNotExist) {
		return s.readFromFile(file, logger)
	}

	return s.getFromAPI(client, logger)
}

func (s securityProvider) readFromFile(file *os.File, logger *zap.Logger) ([]*tradeapi.Security, error) {
	content, err := os.ReadFile(securityProviderFile)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if err := file.Close(); err != nil {
		logger.Error("Failed to close file", zap.Error(err))
	}

	var securities []*tradeapi.Security
	if err := json.Unmarshal(content, &securities); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return securities, nil
}

func (s securityProvider) getFromAPI(
	client finamclient.IFinamClient,
	logger *zap.Logger,
) ([]*tradeapi.Security, error) {
	securities, err := client.GetSecurities()
	if err != nil {
		return nil, fmt.Errorf("get securities: %w", err)
	}
	file, err := os.Create(securityProviderFile)
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Error("Failed to close file", zap.Error(err))
		}
	}()

	content, err := json.Marshal(securities.GetSecurities())
	if err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	if _, err = file.Write(content); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	return securities.GetSecurities(), nil
}
