package cryptochain

import (
	"errors"
	"sync"
)

// CryptoChain is the SHA-256-linked, append-only ledger of ValuationTokens
// for a single property. Concurrent reads are cheap (RWMutex); writes are
// serialised. Tokens are stored by value — once Append returns, the chain
// holds an immutable snapshot.
type CryptoChain struct {
	cfg ChainConfig

	mu     sync.RWMutex
	tokens []ValuationToken
}

// ErrZeroValueToken is returned by Append when given the zero value of
// ValuationToken — that is almost certainly a programming mistake.
var ErrZeroValueToken = errors.New("cryptochain: cannot append zero-value token")

// NewChain constructs an empty CryptoChain seeded with the supplied
// FrozenParameters template.
func NewChain(cfg ChainConfig) *CryptoChain {
	return &CryptoChain{cfg: cfg}
}

// Append finalises and stores a ValuationToken.
//
// Caller-supplied fields:
//   - PropertyID, Timestamp, ConditionedDataHash, IndicatorsHash, RTPMV, Currency
//
// Chain-managed fields (overwritten on append, returned filled-in):
//   - TokenID         — assigned as len(chain) + 1
//   - PrecedingHash   — ZeroHash on genesis, otherwise the previous TokenHash
//   - TokenHash       — SHA-256 over the finalised token
//
// Returns ErrZeroValueToken when the input is the zero value of
// ValuationToken.
func (c *CryptoChain) Append(t ValuationToken) (ValuationToken, error) {
	if t == (ValuationToken{}) {
		return ValuationToken{}, ErrZeroValueToken
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	t.TokenID = uint64(len(c.tokens) + 1)
	if len(c.tokens) == 0 {
		t.PrecedingHash = ZeroHash
	} else {
		t.PrecedingHash = c.tokens[len(c.tokens)-1].TokenHash
	}
	t.TokenHash = HashToken(t)

	c.tokens = append(c.tokens, t)
	return t, nil
}

// Head returns the most recently appended token. ok is false when the
// chain is empty.
func (c *CryptoChain) Head() (ValuationToken, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.tokens) == 0 {
		return ValuationToken{}, false
	}
	return c.tokens[len(c.tokens)-1], true
}

// Len returns the number of tokens currently in the chain.
func (c *CryptoChain) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.tokens)
}

// GetToken returns the token at the given zero-based index. ok is false
// when the index is out of range.
func (c *CryptoChain) GetToken(index int) (ValuationToken, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if index < 0 || index >= len(c.tokens) {
		return ValuationToken{}, false
	}
	return c.tokens[index], true
}

// ToAuditMode produces an AuditRecord per token, each carrying the
// chain's frozen parameters. Returns a fresh slice — safe for callers
// to retain or modify.
func (c *CryptoChain) ToAuditMode() []AuditRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]AuditRecord, len(c.tokens))
	for i, t := range c.tokens {
		out[i] = AuditRecord{
			Token:            t,
			FrozenParameters: c.cfg.FrozenParameters,
		}
	}
	return out
}
