package cryptochain

// Verify walks the chain and confirms two invariants for every token:
//
//  1. Self-integrity: HashToken(token) equals the stored TokenHash.
//     This catches any field tamper because every field except TokenHash
//     itself is part of the hash input.
//
//  2. Linkage:        token.PrecedingHash equals the previous token's
//     stored TokenHash (or ZeroHash on the genesis token). This catches
//     the case where an attacker tampers with a token AND recomputes
//     TokenHash to match — the back-pointer to the prior token still
//     diverges from what the prior token actually hashes to.
//
// BrokenAtIndex is the lowest index that fails either check; -1 means
// the chain is intact. PerToken[i] is true iff token i passed both checks.
//
// Empty chains are reported as valid.
func Verify(chain *CryptoChain) VerificationReport {
	chain.mu.RLock()
	defer chain.mu.RUnlock()

	n := len(chain.tokens)
	report := VerificationReport{
		PerToken:      make([]bool, n),
		ChainValid:    true,
		BrokenAtIndex: -1,
	}

	for i := 0; i < n; i++ {
		t := chain.tokens[i]
		ok := true

		if HashToken(t) != t.TokenHash {
			ok = false
		}

		var expectedPreceding string
		if i == 0 {
			expectedPreceding = ZeroHash
		} else {
			expectedPreceding = chain.tokens[i-1].TokenHash
		}
		if t.PrecedingHash != expectedPreceding {
			ok = false
		}

		report.PerToken[i] = ok
		if !ok && report.BrokenAtIndex == -1 {
			report.BrokenAtIndex = i
			report.ChainValid = false
		}
	}

	return report
}
