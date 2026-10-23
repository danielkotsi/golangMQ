# BENCH-003: Role carving ignores the documented global channel order

- **Status:** Fixed
- **Severity:** Medium
- **Component:** `internal/bench/topology.go`
- **Failing test:** `internal/bench/harness_bulletproof_test.go::TestTopology_RoleCarvingUsesGlobalIndexOrder`

## Reproduction

```bash
go test ./internal/bench \
  -run TestTopology_RoleCarvingUsesGlobalIndexOrder \
  -count=1 -v
```

Test configuration: `conns=2`, `channels=6`, `pub-channels=2`,
`cons-channels=2`, even placement.

## Expected

The manual defines a deterministic global channel index `0..channels-1`
(`docs/benchmark-manual.md:55`) and says mixed slots are carved in global-index
order (`docs/benchmark-manual.md:69-71`):

```text
global:  0  1  2  3  4  5
role:    P  P  C  C  B  B
publish-capable: 0 1 4 5
consume-capable: 2 3 4 5
```

## Observed

Actual placement is connection-major:

```text
conn0 ch0 global0 publish
conn0 ch1 global2 publish
conn0 ch2 global4 consume
conn1 ch0 global1 consume
conn1 ch1 global3 both
conn1 ch2 global5 both

roles:          P{0,2} C{4,1} B{3,5}
publish-capable: 0,2,3,5
consume-capable: 4,1,3,5
```

The test fails at the first mismatch:

```text
global channel 2 (conn 0 ch 1) role=publish want consume
```

## Root cause

Even channel placement builds per-connection slices by iterating global indices
and appending index `i` to connection `i%n`:

```go
for i := 0; i < c.Channels; i++ {
    chans[i%n] = append(chans[i%n], i)
}
```

`buildTopology` then visits connections in ascending order and appends to
`allSlots` connection by connection. For `channels=6, conns=2`, `allSlots`
becomes global indices `0,2,4,1,3,5`, not sorted global order. The comment above
`allSlots` claims global order for every placement mode, but that is false for
even placement when channels are distributed round-robin across connections.

Role stamping iterates `allSlots`, so the first two mixed slots are global `0`
and `2`, producing `P{0,2}` instead of `P{0,1}`. Capability-list construction
also inherits the resulting connection-major order.

## Impact

- Role assignment contradicts the documented topology contract.
- `publish-capable` and `consume-capable` order differs from the manual/golden
  expectations.
- Worker placement follows the wrong global slot order, which changes which
  channels carry load.
- Golden topology or plan-output tests based on the manual will disagree.
- The topology comment is materially incorrect, making future maintenance risky.

## Suggested fix direction

- Sort mixed slots by `GlobalIndex` before role carving, or build `allSlots` in
  global-index order regardless of connection placement.
- Build `publishCapable` and `consumeCapable` in documented global-index order.
- Update the misleading `allSlots` comment and add a regression assertion for
  even, uneven, and separated topologies.

## Resolution

Fixed in `internal/bench/topology.go`. The placement slice is sorted by global
index before mixed-slot role carving and capability-list construction. Both
`publish-capable` and `consume-capable` are now emitted in documented global-index
order.
