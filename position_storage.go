package fnmbroker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/evsamsonov/trengin/v2"
)

const (
	positionStorageDeleteTimeout          = 5 * time.Minute
	positionStorageClosedPositionLifetime = 5 * time.Minute
)

type positionStorage struct {
	mu                     sync.RWMutex
	list                   map[trengin.PositionID]*finamPosition
	deleteTimeout          time.Duration
	closedPositionLifetime time.Duration
}

func newPositionStorage() *positionStorage {
	return &positionStorage{
		list:                   make(map[trengin.PositionID]*finamPosition),
		deleteTimeout:          positionStorageDeleteTimeout,
		closedPositionLifetime: positionStorageClosedPositionLifetime,
	}
}

func (s *positionStorage) Run(ctx context.Context) error {
	for {
		s.deleteClosedPositions()

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.deleteTimeout):
		}
	}
}

func (s *positionStorage) Store(pos *finamPosition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.list[pos.position.ID] = pos
}

func (s *positionStorage) Load(id trengin.PositionID) (*finamPosition, func(), error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pos, ok := s.list[id]
	if !ok || pos.position.IsClosed() {
		return &finamPosition{}, func() {}, errors.New("position not found")
	}
	pos.mu.Lock()
	return pos, func() { pos.mu.Unlock() }, nil
}

func (s *positionStorage) ForEach(f func(pos *finamPosition) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	apply := func(pos *finamPosition) error {
		pos.mu.Lock()
		defer pos.mu.Unlock()

		if pos.position.IsClosed() {
			return nil
		}
		if err := f(pos); err != nil {
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

func (s *positionStorage) deleteClosedPositions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, p := range s.list {
		if p.position.IsClosed() && time.Since(p.position.CloseTime) > s.closedPositionLifetime {
			delete(s.list, k)
		}
	}
}
