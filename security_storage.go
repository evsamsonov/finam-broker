package fnmbroker

import (
	"errors"
	"fmt"

	"github.com/evsamsonov/FinamTradeGo/v2/tradeapi"
)

type securityStorage map[string]*tradeapi.Security

func newSecurityStorage(securities []*tradeapi.Security) securityStorage {
	s := make(securityStorage, len(securities))
	for _, sec := range securities {
		s[s.key(sec.Board, sec.Code)] = sec
	}
	return s
}

func (s securityStorage) Get(board, code string) (*tradeapi.Security, error) {
	security, ok := s[s.key(board, code)]
	if !ok {
		return nil, errors.New("security not found")
	}
	return security, nil
}

func (s securityStorage) key(board, code string) string {
	return fmt.Sprintf("%s:%s", board, code)
}
