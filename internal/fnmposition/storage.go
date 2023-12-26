package fnmposition

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/evsamsonov/trengin/v2"
)

type Storage struct {
	mtx                    sync.RWMutex
	list                   map[trengin.PositionID]*Position
	deleteTimeout          time.Duration
	closedPositionLifetime time.Duration
}

func NewStorage() *Storage {
	return &Storage{
		list:                   make(map[trengin.PositionID]*Position),
		deleteTimeout:          5 * time.Minute,
		closedPositionLifetime: 5 * time.Minute,
	}
}

func (s *Storage) Run(ctx context.Context) error {
	for {
		s.deleteClosedPositions()
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.deleteTimeout):
		}
	}
}

func (s *Storage) Store(pos *Position) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.list[pos.position.ID] = pos
}

func (s *Storage) Load(id trengin.PositionID) (*Position, func(), error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	pos, ok := s.list[id]
	if !ok || pos.position.IsClosed() {
		return &Position{}, func() {}, errors.New("position not found")
	}
	pos.mtx.Lock()
	return pos, func() { pos.mtx.Unlock() }, nil
}

func (s *Storage) ForEach(f func(pos *Position) error) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	apply := func(pos *Position) error {
		pos.mtx.Lock()
		defer pos.mtx.Unlock()

		if pos.position.IsClosed() {
			return nil
		}
		if err := f(pos); err != nil {
			pos.mtx.Unlock()
			return err
		}
		return nil
	}

	for _, pos := range s.list {
		if err := apply(pos); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) deleteClosedPositions() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for k, p := range s.list {
		if p.position.IsClosed() && time.Since(p.position.CloseTime) > s.closedPositionLifetime {
			delete(s.list, k)
		}
	}
}
