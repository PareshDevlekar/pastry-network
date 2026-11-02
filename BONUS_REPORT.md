# Failure resilience bonus report

Team: Omkar Salkade and Paresh Devlekar

## Failure model and implementation

Each Pastry peer is an independent Erlang actor. A node failure is an abrupt process death (`exit(Pid, kill)`), with no graceful handoff. Known peers are monitored; dead entries are removed and routing/leaf/neighborhood state is rebuilt from surviving entries. Local process liveness checks also prevent forwarding to an already dead actor. Monitors are limited to retained state entries and removed when entries leave the tables.

A connection failure is a directed, actor-local link restriction. It can persist permanently, expire after a duration in milliseconds, or be healed explicitly. Bidirectional failures are two restrictions. These restrictions govern outgoing transport discovery, state exchanges, and backup writes. The simulation does not terminate either endpoint. Join setup and external client replies/control calls are outside the injected transport model; link failures are injected after join completion.

Every 30–35 seconds, each actor contacts up to four known peers: leaf extremes and two random known peers. State exchange discovers replacement leaf neighbors and prefix-table entries. A detected death schedules an immediate exchange. Repair uses local bounded state rather than a coordinator's membership list. Unlike the paper's specific routing-cell repair query, this implementation merges suitable entries from neighbor snapshots. The test harness alone keeps global membership to check results.

Normal routes forward directly. For a blocked edge, transport discovery sends to reachable known neighbors; a relay sends directly to the intended next-hop actor if possible, otherwise explores its reachable neighbors. Per-packet tokens suppress repeated processing, and visited lists avoid immediate cycles. Tokens expire after ten seconds. Discovery uses only the finite overlay's known connections; it can be expensive during a partition. A two-second receipt timeout retries the original routing step after a dead destination is removed. A live but unreachable destination produces an explicit failure instead of a successful delivery to the wrong owner. The timer is a simulation policy, not a proven bound under arbitrary load.

The object service stores a primary and sends copies asynchronously to its two closest leaf peers. This gives limited data survival in addition to routing resilience. Replica acknowledgment, versioning, migration, and replenishment are not implemented. Tests establish that backup writes have completed before killing the primary.

## Reproducible tests

Run `make test` for the normal routing suite and `make bonus-test` for fault injection. Both compile with warnings treated as errors. The fault suite seeds the coordinator RNG with `{11,22,33}`; actors and scheduling can still change measured hop counts.

| Scenario | Setup and assertions | Observed result |
| --- | --- | --- |
| One abrupt node death | 128 peers; store an object and verify both backups; suspend its owner, send a read toward it, kill it; verify the pending read retries and returns the stored value; repair; check 327 keys against the nearest surviving ID oracle | All 327 probes passed; mean 1.7462 hops; in-flight read and later backup read passed |
| Seven adjacent node deaths | 128 peers; kill seven consecutive IDs; repair; check 327 original-ID/random keys against all 121 survivors | All probes passed; mean 1.7339 hops |
| Permanent bidirectional connection loss | 128 peers; break the direct edge to a known leaf; read the same object via another actor; explicitly heal | Read passed in 2 hops; direct 1-hop read restored |
| Temporary bidirectional connection loss | Block the same edge for 300 ms; read during outage; wait 350 ms and read again | Read passed in 2 hops during outage; 1 hop after expiry |
| Isolated source | 32 peers; permanently disable every outgoing connection from one source; request an object elsewhere | Explicit `unreachable` result within test deadline; no false delivery |

Each post-crash probe writes a value to an independently selected live source and asserts the destination against the global nearest-live-ID oracle. It is a routing correctness test, not a claim that every pre-crash object survives seven simultaneous deaths. Tests wait 1.2 seconds after requesting immediate repair. The in-flight crash case also waits for the two-second transport timeout as needed. Every test has a bounded receive deadline and fails on missing or incorrect results.

The normal suite covers networks through 1,000 peers, exact node IDs, both numeric boundaries, arbitrary keys, cross-peer object retrieval, and routing-state invariants. The README records the largest command-line network run separately from these fault tests. Failure experiments here use at most 128 peers; they do not establish a maximum resilient network size.

## Findings and limits

Local state repair preserved nearest-surviving-node routing in the tested crash cases. Losing seven adjacent peers stresses the eight-neighbor-per-side leaf configuration; this test passed, but does not prove recovery for all patterns. Link detours preserved delivery with one additional successful-path connection in the tested connected overlay. Temporary failures recovered without restarting nodes. An isolated source could not route, and returned an explicit failure.

Hop counts count the successful delivery path, including relay connections. Flood discovery branches, repair exchanges, replication, and acknowledgments generate additional traffic that is not included in that metric. Thus low reported hops should not be interpreted as low total recovery traffic. Repair contacts are capped to avoid a full-neighbor exchange burst at every timer tick.

The tests do not establish arbitrary partition tolerance, recovery after eight or more adjacent deaths, durable writes, concurrent-write consistency, or lossless behavior under every possible crash timing. In particular, a process may die after acknowledging receipt but before forwarding or replying; there is no end-to-end client retry protocol for that window. A source process that dies also loses its pending requests. Within the tested single-VM failure model, the implementation demonstrates peer death recovery, connection detours, automatic link recovery, explicit partition failure, and limited object survival.
