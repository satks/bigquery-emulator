# Golang Architecture Review Specification

*Performance and Latency Optimization Guide*

March 2026

---

## Table of Contents

- [Introduction and Purpose](#introduction-and-purpose)
- [CPU Latency Optimization in Go](#cpu-latency-optimization-in-go)
- [Memory Latency Optimization in Go](#memory-latency-optimization-in-go)
- [I/O Latency Optimization in Go](#io-latency-optimization-in-go)
- [Concurrency and Synchronization in Go](#concurrency-and-synchronization-in-go)
- [Data Architecture Patterns in Go](#data-architecture-patterns-in-go)
- [Eliminating Work in Go](#eliminating-work-in-go)
- [Profiling and Measurement Toolkit](#profiling-and-measurement-toolkit)
- [Production Readiness Checklist](#production-readiness-checklist)
- [Appendix: Quick Reference](#appendix-quick-reference)

---

## Introduction and Purpose

This Golang Architecture Review Specification provides a comprehensive framework for evaluating and optimizing Go codebases through the lens of latency principles. The guide bridges foundational latency concepts with Go-specific implementation patterns and tools.

### Purpose of This Architecture Review Spec

- Establish a systematic approach to reviewing Go applications for latency bottlenecks and performance issues
- Cross-apply latency principles (CPU, memory, I/O, concurrency) to Go-specific best practices
- Provide actionable checklists for architecture reviews at each layer
- Help developers measure, profile, and optimize tail latency in production systems

### How to Use This Document

For each section, review the latency principle overview, then apply the Go-specific checklist items to your codebase. Use the profiling and measurement toolkit section to validate improvements. The production readiness checklist summarizes all critical items across categories.

### Latency Fundamentals Recap

- **Latency:** Time elapsed between initiating a request and receiving a response
- **Tail latency (p99, p999):** Worst-case latency percentiles that most impact user experience
- **Latency vs throughput tradeoff:** Reducing latency often requires sacrificing throughput and vice versa
- **Latency is additive:** Bottlenecks compound across layers; optimization requires identifying critical paths

---

## CPU Latency Optimization in Go

CPU latency is determined by goroutine scheduling, cache efficiency, branch prediction, and syscall overhead. Go developers can optimize CPU latency by understanding scheduler behavior, managing goroutine pools, and aligning data structures with CPU cache lines.

### Goroutine Scheduling

- **Check GOMAXPROCS setting:** Verify it is intentionally set for your workload
- **Quantify goroutine count:** Use `runtime.NumGoroutine()` in tests and production; avoid unbounded spawning
- **Detect goroutine leaks:** Log goroutine count at startup and after key operations; look for monotonic increases
- **Use pprof goroutine profile:** `go tool pprof http://localhost:6060/debug/pprof/goroutine` to find leaking goroutines

### CPU Cache Awareness

- **Understand cache line size:** 64 bytes on most modern CPUs; struct fields accessed together should fit within one line
- **Order struct fields by size:** Group fields accessed together; minimize padding; consider alignment effects on performance
- **Prevent false sharing:** Pad shared fields to cache line boundaries (64 bytes) to avoid contention between cores
- **Example:** A counter accessed by multiple goroutines should be padded to 64 bytes to avoid false sharing

### Branch Prediction

- **Avoid unpredictable branching in hot loops:** Use early exit and sorted data for consistent branch direction
- **Profile branch misses:** Use pprof with specific perf events (`go tool pprof` on flamegraphs with branch data)
- **Sort data before processing:** Branchy algorithms run faster on sorted input due to improved prediction

### Context Switching

- **Minimize syscalls:** Batch I/O operations; use buffered I/O (`bufio` package) to reduce context switches
- **Use `runtime.LockOSThread()`:** For latency-critical goroutines, lock to an OS thread to avoid scheduler context switches
- **Monitor block profile:** `go tool pprof http://localhost:6060/debug/pprof/block` to detect scheduler contention

### CPU-Specific Anti-patterns

- **Excessive goroutine creation:** Spawning goroutines per request without pooling
- **Unbounded goroutine spawning:** No limits on concurrent work; leads to scheduler degradation
- **CPU-bound work without yielding:** Long-running functions that block scheduler progress

---

## Memory Latency Optimization in Go

Memory latency depends on allocation patterns, garbage collector behavior, and data structure layout. Go developers can reduce memory latency by optimizing for stack allocation, tuning GC settings, and using object pooling.

### Heap vs Stack Allocation

- **Run escape analysis:** `go build -gcflags='-m'` to identify heap escapes; use `-m=2` for detailed analysis
- **Keep allocations on the stack:** Stack allocation is effectively free; heap allocation requires GC work
- **Avoid returning pointers to local variables:** Forces allocation to heap; return values directly when possible
- **Example:** `func Sum(a, b int) int` (stack) vs `func Sum(a, b int) *int` (heap)

### Garbage Collector Tuning

- **Tune GOGC:** Controls GC frequency; higher values (e.g., 200) reduce GC overhead but increase pause times
- **Set GOMEMLIMIT:** Caps heap size to trigger more frequent GC; prevents unbounded growth in memory usage
- **Monitor GC pauses:** Use trace tool (`go tool trace`) to measure stop-the-world pause durations; target < 100ms for tail latency
- **Profile GC behavior:** `go test -bench=. -benchmem` tracks allocations; look for alloc/op trends

### sync.Pool Usage

- **Reuse objects with `sync.Pool`:** Reduces allocation rate for frequently created objects (buffers, parsers)
- **Pool sizing:** Set initial pool size to avoid thundering herd; use `Put()` during initialization if possible
- **Profile pool efficiency:** Check if pooled objects are being reused; low hit rate indicates underutilized pooling

### Memory Allocation Patterns

- **Pre-allocate slices:** Use `make([]T, 0, capacity)` to avoid repeated allocations and copies during `append()`
- **Pre-allocate maps:** Use `make(map[K]V, capacity)` to avoid rehashing during insertions
- **Avoid append in hot loops:** Pre-size slices; if growth is needed, use a separate allocation function

### Struct Layout Optimization

- **Order fields by size (descending):** `uint64`, `uint32`, `uint16`, `uint8` to minimize padding
- **Group frequently accessed fields:** Improve cache locality by placing related fields together
- **Measure struct size:** `unsafe.Sizeof()` to verify layout; use pprof memory profile to check for excessive padding

### False Sharing Prevention

- Pad shared mutable fields to cache line boundaries (64 bytes)
- Use `atomic.Value` for read-mostly shared data; reduces false sharing compared to mutex-protected fields

### Memory-Specific Anti-patterns

- **Pointer-heavy data structures:** Linked lists vs slices; slices are more cache-friendly
- **Excessive `interface{}` usage:** Forces heap allocation; use concrete types when possible
- **String concatenation in loops:** Builds multiple intermediate strings; use `strings.Builder` instead
- **Unbounded object retention:** Caches without eviction policies lead to memory bloat

---

## I/O Latency Optimization in Go

I/O latency is the largest contributor to end-to-end latency in most systems. Go's netpoller and async I/O capabilities help minimize blocking, but careful architecture is needed to avoid common pitfalls.

### Network I/O

- **Implement connection pooling:** Reuse TCP connections; avoid creating connections per request
- **Enable HTTP keep-alive:** Set `Connection: keep-alive` in request headers
- **Disable Nagle's algorithm:** Set `TCP_NODELAY` on TCP connections for low-latency scenarios; `net.Dialer.Control()` in Go
- **Tune buffer sizes:** Socket read/write buffers affect throughput; typical 64KB is reasonable
- **Profile network latency:** Use `netstat` or `ss` to check socket states; `tcpdump` to capture packet traces

### Disk I/O

- **Prefer sequential access:** Random disk I/O can be 1000x slower than sequential; structure data for streaming
- **Use buffered I/O:** `bufio.Reader` and `bufio.Writer` reduce syscall overhead; typical 64KB buffer is good
- **Consider memory-mapped files:** `mmap` for read-heavy workloads; OS page cache is often faster than application buffering
- **Batch writes:** Use `fsync` sparingly; cluster writes to reduce sync overhead

### Syscall Overhead

- **Batch syscalls:** Multiple small reads/writes incur overhead; use buffered I/O to batch operations
- **Use `io.Copy` for zero-copy transfers:** Avoids reading into userspace; kernel handles data movement directly
- **Avoid unnecessary file stat calls:** Cache file metadata; `stat()` is a syscall and should be infrequent

### Serialization

- **Profile serialization overhead:** JSON can be 10-100x slower than binary formats; measure and choose wisely
- **Use streaming decoders:** `json.Decoder` reads from `io.Reader`; avoids buffering entire payload in memory
- **Consider Protocol Buffers or FlatBuffers:** Faster than JSON for latency-sensitive APIs; smaller message sizes
- **Lazy deserialization:** Parse only fields needed; avoid unmarshaling unused fields

### Network Stack Understanding

- **Understand Go netpoller:** Go uses epoll-based async I/O; goroutines blocked on I/O do not consume OS threads
- **Tune connection timeouts:** `net.Dialer` with `Timeout` prevents hanging connections; typical 5-30 seconds for remote services
- **Implement DNS caching:** DNS lookups add 10-100ms; use custom resolver with caching or query once and pool results

### I/O-Specific Anti-patterns

- **Unbuffered I/O:** Each read/write is a syscall; use `bufio` for batching
- **Opening/closing connections per request:** Connection establishment takes multiple RTTs
- **Synchronous I/O in hot paths:** Use channels and goroutines for parallelism
- **No timeout on network operations:** Can hang indefinitely; always set deadline

---

## Concurrency and Synchronization in Go

Go's concurrency primitives (goroutines, channels, sync package) are powerful but require careful design to avoid deadlocks, priority inversion, and convoying. Proper synchronization is critical to tail latency.

### Channel Patterns

- **Choose buffered vs unbuffered wisely:** Unbuffered blocks sender; buffered decouples sender and receiver
- **Size buffers appropriately:** Small buffers cause back-pressure; oversized buffers hide latency issues
- **Use `select` with `default` for non-blocking sends:** Prevents goroutine blocking; allows time-bounded operations
- **Close channels from sender only:** Prevents panic on send to closed channel; use `sync.WaitGroup` to signal completion

### Mutex Optimization

- **Choose `sync.Mutex` vs `sync.RWMutex`:** RWMutex is slower for mostly-write workloads; use Mutex for balanced access
- **Keep critical sections short:** Lock, do minimal work, unlock immediately
- **Minimize lock contention:** Use lock-free structures or finer-grained locking for frequently accessed data
- **Profile lock contention:** `go tool pprof http://localhost:6060/debug/pprof/mutex`

### Atomic Operations

- **Use `sync/atomic` for simple counters and flags:** Avoids mutex overhead; provides lock-free updates
- **Use `atomic.Value` for read-heavy config:** Multiple readers can access without contention; atomic updates
- **Profile atomic contention:** Measure compare-and-swap retry rates to detect high contention

### Lock-Free Patterns

- **Use channels as semaphores:** More efficient than mutex for coordinating work across goroutines
- **Copy-on-write pattern:** Read is fast (no locking); writes are expensive but infrequent; useful for mostly-read config

### Deadlock Prevention

- **Enforce consistent lock ordering:** Always acquire locks in the same order to prevent circular waits
- **Use `context.WithTimeout` for bounded waits:** Prevents indefinite blocking; allows graceful timeout handling
- **Test with race detector:** `go test -race` to detect data races and potential deadlocks

### Priority Inversion and Convoying

- **Recognize convoying:** Multiple goroutines blocked on same lock; tail latency increases as queued work backs up
- **Mitigation:** Use lock-free data structures, atomic operations, or channels to reduce contention
- **Profile with block profiling:** `go tool pprof http://localhost:6060/debug/pprof/block`

### Concurrency-Specific Anti-patterns

- **Channel misuse as mutex:** Channels serialize work; not appropriate for protecting data
- **Mutex in hot loops:** High contention; optimize by batching or lock-free structures
- **Goroutine leaks from abandoned channels:** Always close channels or use `context.WithCancel()`
- **Unbounded concurrency:** No semaphore or worker pool limits; leads to resource exhaustion

---

## Data Architecture Patterns in Go

Caching, partitioning, and replication are architectural patterns that significantly impact latency. Go developers must understand cache strategies, eviction policies, and data colocation principles.

### In-Process Caching Strategies

- **Use `groupcache` for distributed caching:** Avoids thundering herd; automatically fetches from peer on miss
- **Consider `bigcache` for large caches:** Reduces GC overhead with custom memory layout; suitable for caches > 100MB
- **Evaluate `ristretto` for high-performance caching:** Provides automatic cost-based eviction; low memory overhead
- **Profile cache hit rate:** Track hits/misses; low hit rate indicates poor cache strategy or small working set

### Cache Patterns

- **Cache-aside:** Application checks cache, fetches from origin if miss; simple but requires application logic
- **Read-through:** Cache layer fetches from origin transparently; cleaner separation but more complex to implement
- **Write-through vs write-behind:** Write-through is slower but consistent; write-behind is faster but risks loss

### Cache Hit Ratio Optimization

- **Analyze working set:** Measure unique keys accessed; size cache to fit working set
- **Tune TTL:** Balance freshness vs hit rate; longer TTL increases hits but stales data
- **Choose eviction policy:** LRU (evict least recently used), LFU (least frequently), SIEVE (recent wins); measure impact
- **Monitor eviction rate:** High eviction indicates cache thrashing; increase cache size or improve TTL

### Data Partitioning

- **Implement sharding:** Distribute load across multiple partitions; reduces contention per partition
- **Use consistent hashing:** Minimizes key redistribution when adding/removing partitions; essential for scalability
- **Balance shard distribution:** Uneven shards lead to hot spots; monitor and rebalance if necessary

### Replication Considerations

- **Use read replicas for database clients:** Spreads read load; improves latency by distributing across replicas
- **Maintain separate connection pool per replica:** Avoids contention on single connection pool; enables per-replica tuning
- **Implement read preference:** Route to nearest/fastest replica; reduces latency for geographically distributed systems

### Data Colocation

- **Embed SQLite for low-latency local data:** Avoids network round-trips; suitable for read-heavy reference data
- **Reduce network hops:** Fetch related data in single query; join on client side to minimize round-trips

### Data Architecture Anti-patterns

- **Unbounded caches:** No eviction policy; memory grows indefinitely; leads to OOM or GC pauses
- **Cache stampede:** Multiple requests on cache miss overwhelm origin; use `singleflight` to deduplicate
- **Missing negative caching:** Failed lookups are retried repeatedly; cache negative results with short TTL
- **No cache invalidation strategy:** Stale data leads to correctness issues and user complaints

---

## Eliminating Work in Go

The fastest code is code that never runs. By reducing algorithmic complexity, precomputing results, and memoizing expensive operations, developers can dramatically improve latency.

### Algorithmic Complexity

- **Choose appropriate data structures:** `map` for O(1) lookups, `slice` for sequential access; avoid O(n) when O(log n) is possible
- **Avoid O(n^2) in hot paths:** Use `BenchmarkXxx` tests to detect algorithmic slowdowns early
- **Profile CPU time:** `go tool pprof` identifies which functions consume most CPU; focus optimization there

### Precomputation

- **Use `init()` functions for lookup tables:** Populate expensive data structures at startup, not request time
- **Use `go generate` for code generation:** Pre-generate lookup tables, marshalers, or other boilerplate at build time

### Memoization

- **Cache expensive function results:** Store results of deterministic functions; reuse across requests
- **Use `singleflight` for deduplication:** `golang.org/x/sync/singleflight` prevents redundant computation; essential for cache misses

### Lazy Evaluation

- **Use `sync.Once` for deferred initialization:** Compute expensive data only when first accessed; thread-safe
- **Lazy loading patterns:** Delay expensive operations until needed; can improve startup time

### Reducing Serialization

- **Avoid unnecessary marshal/unmarshal:** Cache marshaled form; update only on changes
- **Use binary protocols for internal services:** JSON is convenient but slow; Protocol Buffers/MessagePack are faster

### Elimination Anti-patterns

- **Redundant JSON encoding/decoding:** Serialize once, reuse instead of re-marshaling
- **Reflection in hot paths:** Reflection is slow; use code generation or specific types
- **Regexp compilation in loops:** Compile once with `regexp.MustCompile()` at package level
- **Unnecessary allocations:** Every allocation is work; profile and eliminate where possible

---

## Profiling and Measurement Toolkit

Effective optimization requires measurement. Go's pprof tool, benchmarking support, and runtime tracing capabilities provide comprehensive visibility into application behavior.

### pprof Integration

- **CPU profiling:** `go tool pprof http://localhost:6060/debug/pprof/profile` to identify CPU hotspots
- **Memory profiling:** `go tool pprof http://localhost:6060/debug/pprof/heap` tracks allocation sites
- **Goroutine profiling:** `go tool pprof http://localhost:6060/debug/pprof/goroutine` detects leaks
- **Block profiling:** `go tool pprof http://localhost:6060/debug/pprof/block` identifies scheduler contention
- **Mutex profiling:** `go tool pprof http://localhost:6060/debug/pprof/mutex` (Go 1.19+) reveals lock contention

### Benchmarking Best Practices

- **Write `testing.B` benchmarks:** `go test -bench=. -benchmem` to measure throughput and allocations
- **Use sub-benchmarks:** Run multiple scenarios in single benchmark; enables comparative analysis
- **Track memory allocations:** `b.ReportAllocs()` to measure alloc/op; target zero allocations in hot paths
- **Compare benchmarks:** `benchstat` tool compares before/after performance; detects regressions

### Tracing and Visualization

- **Runtime tracing:** `go tool trace` shows execution timeline; identifies goroutine scheduling, GC pauses, and I/O blocking
- **Distributed tracing:** OpenTelemetry integration for cross-service tracing; essential for microservices
- **Flame graphs:** Generate from pprof data (`pprof -http=:8080`); visualizes CPU time distribution

### Latency Measurement

- **Histogram-based tracking:** Use libraries like go-echarts or Prometheus histograms to track percentiles
- **Target metrics:** p50 (median), p99 (99th percentile), p999 (99.9th percentile); focus on tail latency
- **Synthetic load testing:** Load test with realistic traffic patterns; measure latency distribution under load

### Production Profiling

- **Low-overhead continuous profiling:** Use pprof with sampling (CPU: 1%, Memory: always) to avoid overhead
- **Integrate into CI/CD:** Automatically detect performance regressions via benchmarks and profiling

### Key Metrics Reference

- **Allocation rate:** alloc/op from benchmarks; target < 1 alloc/op in hot paths
- **GC pause time:** Measure with `runtime/trace`; target < 100ms for tail latency
- **Goroutine count:** Monitor with `runtime.NumGoroutine()`; target stable or predictable growth
- **Lock contention:** Mutex profile reveals blocked time; minimize critical sections

---

## Production Readiness Checklist

This table summarizes all critical review items across categories. Use this as a final checklist before deploying to production.

| Category | Review Item | Severity | Go-Specific Tool |
|---|---|---|---|
| CPU | GOMAXPROCS tuning | High | `export GOMAXPROCS`; `runtime.GOMAXPROCS()` |
| CPU | Goroutine count monitoring | High | `runtime.NumGoroutine()`; pprof goroutine profile |
| CPU | Goroutine leak detection | Critical | pprof goroutine profile; look for monotonic increases |
| CPU | Struct field ordering | Medium | `unsafe.Sizeof()`; pprof memory profile |
| CPU | Cache-line padding | Medium | Manual review; benchmark for impact |
| CPU | Branch prediction optimization | Medium | `go tool pprof` with perf data; flamegraphs |
| CPU | Syscall minimization | High | `strace` or syscall profiling |
| CPU | Lock usage in hot paths | High | pprof mutex profile |
| Memory | Escape analysis review | High | `go build -gcflags='-m'` or `'-m=2'` |
| Memory | Heap vs stack allocation | High | pprof memory profile; alloc/op from benchmarks |
| Memory | GOGC tuning | High | `export GOGC=`; `runtime/trace` analysis |
| Memory | GOMEMLIMIT configuration | High | `export GOMEMLIMIT=`; GC pauses |
| Memory | GC pause analysis | Critical | `go tool trace`; target < 100ms |
| Memory | sync.Pool effectiveness | Medium | pprof alloc/op; manual code review |
| Memory | Slice/map pre-allocation | Medium | pprof alloc/op; benchmarks |
| Memory | False sharing mitigation | Medium | Benchmark before/after; pprof CPU profile |
| I/O | Connection pooling | Critical | Connection metrics; `netstat`/`ss` |
| I/O | HTTP keep-alive enabled | High | `net.Client` with connection pooling |
| I/O | TCP_NODELAY configuration | High | `net.Dialer.Control()` callback |
| I/O | Buffered I/O usage | High | `bufio.Reader`/`Writer`; syscall count |
| I/O | DNS caching | High | `net.Resolver` with custom caching |
| I/O | Serialization format choice | High | Benchmark JSON vs binary formats |
| I/O | Streaming decoders | Medium | `json.Decoder`; streaming JSON |
| I/O | Zero-copy transfers | Medium | `io.Copy` for file transfers |
| I/O | Connection timeouts | Critical | `net.Dialer` with `Timeout`/`Deadline` |
| Concurrency | Channel sizing | High | Benchmark buffered vs unbuffered; measure back-pressure |
| Concurrency | Mutex vs RWMutex choice | High | pprof mutex profile; benchmark |
| Concurrency | Lock granularity | High | Code review; pprof block profile |
| Concurrency | Atomic operations usage | Medium | `sync/atomic` for counters; `atomic.Value` |
| Concurrency | Deadlock prevention | Critical | `go test -race`; consistent lock ordering |
| Concurrency | Goroutine leak tests | Critical | Benchmark goroutine count delta |
| Concurrency | singleflight usage | High | `golang.org/x/sync/singleflight` for cache misses |
| Data | Cache hit ratio analysis | High | Metrics collection; monitor hits/misses |
| Data | Cache eviction policy | Medium | Choose LRU/LFU/SIEVE; benchmark |
| Data | TTL tuning | Medium | Balance freshness vs hit rate |
| Data | Sharding strategy | High | Consistent hashing implementation |
| Data | Replication setup | Medium | Separate connection pools per replica |
| Data | Negative caching | Medium | Cache failed lookups with short TTL |
| Data | SQLite embedding | Medium | Evaluate for reference data use case |
| Profiling | pprof endpoints exposed | High | `import _ "net/http/pprof"` |
| Profiling | Benchmark suite | High | `testing.B` benchmarks; `-benchmem` flag |
| Profiling | Production profiling | Medium | Low-overhead sampling in production |
| Profiling | Latency histograms | High | Prometheus or similar for percentiles |
| Profiling | Trace analysis | High | `go tool trace` for GC, scheduler, I/O analysis |

---

## Appendix: Quick Reference

### Go Environment Variables for Performance

| Variable | Purpose | Typical Value |
|---|---|---|
| GOGC | GC trigger percentage | 100 (default) or 200 (lower pause times) |
| GOMEMLIMIT | Heap size limit | 1GiB or 2GiB (depends on workload) |
| GOMAXPROCS | Max OS threads for goroutines | CPU count (auto-detected by default) |
| GODEBUG | Debug options | `gctrace=1`, `gcpause=10000000`, `gcstoptheworld=1` |

### Essential pprof Commands

- **CPU profile:** `go tool pprof http://localhost:6060/debug/pprof/profile`
- **Heap profile:** `go tool pprof http://localhost:6060/debug/pprof/heap`
- **Goroutine profile:** `go tool pprof http://localhost:6060/debug/pprof/goroutine`
- **Block profile:** `go tool pprof http://localhost:6060/debug/pprof/block`
- **Mutex profile:** `go tool pprof http://localhost:6060/debug/pprof/mutex` (Go 1.19+)
- **Allocations:** `go tool pprof http://localhost:6060/debug/pprof/allocs`

### Common `go test -bench` Patterns

- **Basic benchmark:** `go test -bench=. -benchmem`
- **CPU profile:** `go test -bench=. -cpuprofile=cpu.prof`
- **Memory profile:** `go test -bench=. -memprofile=mem.prof`
- **Specific test:** `go test -bench=BenchmarkName`
- **Run tests (not benchmarks):** `go test -run TestName`
- **Verbose output:** `go test -bench=. -v`
- **Count benchmark runs:** `go test -bench=. -count=3` (averages results)

### Latency Numbers Every Go Developer Should Know

| Operation | Latency | Notes |
|---|---|---|
| L1 cache hit | ~1 ns | Fastest; on-core private cache |
| L2 cache hit | ~4 ns | Per-core or shared L2 |
| L3 cache hit | ~12 ns | Shared across cores |
| Main memory (RAM) | ~100 ns | Typical DDR4/DDR5 latency |
| SSD random read | ~100 us | 10-100 us range; depends on drive |
| SSD sequential read | ~10 us per block | Amortized across reads |
| Network RTT (local) | ~1 ms | Same datacenter |
| Network RTT (cross-region) | ~100 ms | Depends on geography |
| Disk random seek + read | ~10 ms | 5-20 ms typical for HDD |
| Disk sequential read | ~100 us per block | Amortized across reads |
| Mutex lock/unlock | ~100 ns | Uncontended; contention adds overhead |
| Atomic operation | ~5 ns | Uncontended; CPU cache hit |
| Context switch | ~1 us | TLB flush, cache invalidation |
| Goroutine creation | ~1 us | Scheduler overhead; stack allocation |
| Channel send/recv | ~100 ns | Uncontended; contention increases |

### References

- Go Documentation: https://golang.org/doc
- pprof Documentation: https://github.com/google/pprof
- Go Memory Model: https://golang.org/ref/mem
- Designing Data-Intensive Applications: Chapters on Latency, Caching, Replication, Partitioning
- Systems Performance: Enterprise and the Cloud by Brendan Gregg
- Mechanical Sympathy Blog: http://mechanicalsympathy.blogspot.com/
