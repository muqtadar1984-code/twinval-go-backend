# internal/compute — Patent Module 340

## What this package does

This is the mathematical core of TwinVal. It implements **Patent Module 340
(Computation Module)** from the Complete Specification filed 13 March 2026,
Application No. 202641030498.

It computes:
1. Five technical indicators (SHF, ESF, USS, PDP, CI)
2. Health Factor (composite of all five)
3. Real-Time Property Market Value (RTPMV)

## Files

| File | Purpose |
|------|---------|
| `types.go` | All data structures — `ConditionedData`, `TechnicalIndicators`, `RTPMV`, `BaselineMarketValue` |
| `indicators.go` | All computation functions — one function per indicator + `ComputeHealthFactor` + `ComputeRTPMV` |
| `indicators_test.go` | Unit tests + parity fixtures for validation against Python POC |

## Key formulas (from patent specification)

```
RTPMV = Land_Value + (Structure_Value × Health_Factor)

Health_Factor = SHF × ESF × (1 − USS) × PDP × CI
```

## Rules for Claude Code

1. **Do not change default config values** in `DefaultSHFConfig()`,
   `DefaultESFConfig()`, etc. without also updating `indicators_test.go`
   parity fixtures. These values must match the Python POC exactly.

2. **Do not reorder the Health Factor formula**. The `(1 − USS)` inversion
   is intentional — USS measures stress (higher = worse), so it must be
   inverted before multiplication.

3. **All indicator functions must return values in [0.0, 1.0]**. The
   `clamp()` helper enforces this. Never remove clamp calls.

4. **The Land_Value in RTPMV must never be multiplied by Health Factor**.
   Only Structure_Value is adjusted. This is a core patent claim.

5. **Wöhler S-N curve in ComputeSHF** — the `wohlerPenalty()` function
   implements a non-linear fatigue model. Low readings below the normal
   ceiling produce zero penalty. Do not linearise this.

6. **Effective age in ComputePDP** can decrease below chronological age
   when `ConditionQuality > 0.5`. This is the maintenance incentive
   mechanism described in patent paragraph [0070]. Do not clamp it
   to chronological age as a floor.

## How to run tests

```bash
cd internal/compute
go test ./... -v
```

## Parity validation against Python POC

Before deploying any changes:

1. Run the Python POC simulator with `degradedPropertyData()` inputs
2. Record the output for SHF, ESF, USS, PDP, CI, Health Factor
3. Paste those values into the `parityFixtures` map in `indicators_test.go`
4. Run `go test ./... -v` — all parity tests must pass

Any difference beyond `1e-9` is a computation error that must be fixed
before the Go backend can replace the Python POC.

## Dependencies

None beyond the Go standard library (`math`). This package must remain
dependency-free to ensure portability across deployment environments.
