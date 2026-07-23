package fnmbroker

// security holds instrument parameters required for trading.
type security struct {
	Symbol   string // ticker@mic, e.g. SBER@MISX
	Board    string // trading board, e.g. TQBR
	Code     string // ticker, e.g. SBER
	Mic      string
	Decimals int32
	LotSize  int64
}

func (s *security) pieces(lots int64) int64 {
	if s.LotSize <= 0 {
		return lots
	}
	return lots * s.LotSize
}

func (s *security) lots(pieces float64) int64 {
	if s.LotSize <= 0 {
		return int64(pieces)
	}
	return int64(pieces) / s.LotSize
}
