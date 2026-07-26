package secrets

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
)

type EnvVault struct {
	mu      sync.RWMutex
	secrets map[string]*Secret
}

func NewEnvVaultFromEnv(prefix string) *EnvVault {
	e := &EnvVault{secrets: map[string]*Secret{}}
	if prefix == "" {
		prefix = "SECRETS"
	}
	pfx := prefix + "_"
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, pfx) {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		name := kv[len(pfx):eq]
		val := kv[eq+1:]
		e.secrets[name] = &Secret{Data: map[string]string{"value": val}, Version: "env"}
	}
	return e
}

var ErrEnvVaultMissing = errors.New("secrets: env vault path not found")

func (e *EnvVault) Read(ctx context.Context, path string) (*Secret, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.secrets[path]
	if !ok {
		return nil, ErrSecretNotFound
	}
	cp := *s
	return &cp, nil
}

func (e *EnvVault) Revoke(ctx context.Context, leaseID string) error { return nil }
