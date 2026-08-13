package fasthttp

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrServerClosed = errors.New("server closed")

func (s *Server) ShutdownWithContext(ctx context.Context) error {
	s.mu.Lock()
	if s.isShutdown {
		s.mu.Unlock()
		return nil
	}
	s.isShutdown = true
	// Close listeners to prevent new connections
	for _, ln := range s.lns {
		ln.Close()
	}
	s.mu.Unlock()

	// Wait for connections to drain outside of the lock
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.connsCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) Serve(ln Listener) error {
	s.mu.Lock()
	if s.isShutdown {
		s.mu.Unlock()
		return ErrServerClosed
	}
	s.mu.Unlock()
	// ... existing serve logic ...
	return nil
}

func (s *Server) AppendCert(cert []byte, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isShutdown {
		return ErrServerClosed
	}
	// ... existing cert logic ...
	return nil
}