# Project 3 — Pastry in Erlang

## Team members

- Omkar Salkade
- Paresh Devlekar

## Build and run

Requires Erlang/OTP with `erl`, `erlc`, and the standard `crypto` application on PATH. No third-party dependencies are needed.

```sh
./project3 1000 10
```

The launcher compiles the source and starts an Erlang VM. Arguments are the positive number of peers and the nonnegative number of requests per peer. Example output:

```text
Average hops: 2.497500
```

Every peer issues its first request when the workload starts, then one request every second until it has issued `numRequests` requests. The coordinator waits for all successful deliveries before exiting. Thus the request phase takes approximately `numRequests - 1` seconds, plus delivery time. Zero requests prints `0.000000`.

## What is working

- One independent Erlang receive-loop actor per peer, plus the coordinating process. Peer state is private to each actor; communication uses messages. No ETS, shared routing registry, tasks, or non-actor parallelism.
- Deterministic, uniformly distributed 128-bit node IDs obtained by truncating SHA-256 hashes of peer numbers. Routing uses hexadecimal digits (`b = 4`).
- Routing tables indexed by shared-prefix length and next digit, up to eight lower and eight higher leaf neighbors, and a neighborhood set of at most sixteen peers.
- Sequential network joins through an existing bootstrap peer. A join is routed toward the new ID, collecting state from the path. The new peer constructs its bounded tables from that state and announces itself to its known peers. Acknowledgments complete the join before the next peer starts. The coordinator does not construct or distribute global routing tables.
- Leaf-range routing, prefix-table routing, and the fallback to a known peer with at least the same prefix and strictly smaller numerical distance. Ties in numerical distance select the smaller node ID.
- A key/string service with `put` and `get`, delivered at the peer numerically closest to the key. Objects are installed after all peers join; each workload request reads a randomly selected installed object.
- Hop counting for the actual peer-to-peer forwards on each workload route. Local delivery is zero hops. Join messages, object initialization, and response messages are excluded from the average, whose denominator is exactly `numNodes * numRequests`.

The implementation follows Sections 2.1–2.4 of [Rowstron and Druschel's Pastry paper](https://rowstron.azurewebsites.net/PAST/pastry.pdf). This is an in-VM overlay simulation: process IDs stand in for network addresses. A deterministic one-dimensional virtual coordinate supplies a proximity metric for selecting routing and neighborhood entries. Bootstrap selection is random, so this simulation does not claim physical network locality optimization. Joins merge suitable entries from the collected snapshots rather than copying one prescribed row at a time.

## Application API

`pastry_node:start(Id, Owner)` creates a peer; `pastry_node:join(Pid, Bootstrap)` initializes it (`none` creates the first peer). `Owner` is retained as metadata.

`pastry_node:route(Pid, Key, Message, ReplyPid)` returns a reference immediately. `Key` is an integer in `[0, 2^128 - 1]`. Supported application messages are `{put, String}` (list or binary) and `get`. The reply is:

```erlang
{delivered, Reference, DestinationId, Result, Hops}
```

A put returns `ok`; a get returns `{ok, String}` or `error` if absent. The application's `deliver` handler is integrated into the peer module. Forwarding and leaf updates are internal to that module rather than exposed as pluggable callbacks. `pastry_node:state(Pid)` is available for inspection and tests; it is not used by workload routing.

## Validation and largest network

```sh
make test
```

Tests build networks of 1, 2, 17, 100, and 1,000 peers. For every node ID, both ID-space boundaries, and 100 additional random keys per network, tests compare delivery against a global nearest-ID oracle used only in the test harness. Each key is written and then read from independently selected peers. Tests also check routing-table prefix/digit invariants and bounded leaf and neighborhood sizes. Compilation treats warnings as errors.

Largest network actually run: **10,000 peers**, with one request per peer (10,000 successful reads), yielding **3.262300 average hops**. A 1,000-peer run with two requests per peer yielded **2.497500**. These are observed runs, not claimed limits; random bootstrap and request choices can change the averages. Single-peer and zero-request runs are also checked.

## Failure bonus

Node crashes, permanent connection failures, and temporary connection failures are implemented and exercised by:

```sh
make bonus-test
```

See [BONUS_REPORT.md](BONUS_REPORT.md) for the failure model, algorithms, measured results, and limits. Peer processes are independent (not linked to one another or to the coordinator); monitored peer deaths trigger local state repair. Every 30–35 seconds, a peer exchanges bounded state with up to four known peers, including its leaf-set extremes. This repairs leaf sets and discovers replacement routing entries without a global membership service.

Failure injection API:

```erlang
exit(PeerPid, kill).  % permanent node crash
pastry_node:block(A, B, permanent). % directed connection failure A -> B
pastry_node:block(A, B, 1000).      % connection unavailable for 1,000 ms
pastry_node:heal(A, B).             % explicitly restore a connection
pastry_node:repair(A).             % request a local repair exchange
```

Block both directions to model an undirected failed connection. Ordinary routing uses direct overlay edges. When a chosen connection is broken, duplicate-suppressed transport discovery through known peers finds a detour to the intended next hop. The destination acknowledges receipt; a two-second timeout retries if that peer has died, or returns `{route_failed, Reference, unreachable}` if the live peer cannot be reached. Successful-path relay connections count toward hops; discovery branches and control messages do not.

Puts also send asynchronous backup copies to the two closest known leaf peers. This supports reads after an owner crash when a backup is the replacement owner. A put acknowledgment confirms the primary write; it does not certify backup durability.

## Limitations

This remains an in-VM simulation. Liveness checks use local Erlang process status and monitors, rather than a real network failure detector. Transport detours use bounded-state flooding rather than the paper's precise failed-routing-cell query algorithm. Recovery is eventual and assumes a connected surviving overlay and enough surviving leaf neighbors. No claim is made for arbitrary partitions or eight or more consecutive node failures.

Objects live in memory. Three copies are not a durable storage protocol: simultaneous loss of all copies loses the value, partitions can prevent backup writes, and backup writes are asynchronous. There is no version reconciliation, automatic re-replication, or migration after later joins. The runner completes joins before storing objects. Concurrent joins/departures and real distributed Erlang deployment are outside the tested scope. A failed request is reported explicitly; the standard no-failure workload still expects every lookup to succeed.
