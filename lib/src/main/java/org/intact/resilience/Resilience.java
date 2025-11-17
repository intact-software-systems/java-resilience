package org.intact.resilience;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Resilience {

    @SuppressWarnings({"unused", "UnusedReturnValue"})
    public record SlidingWindowCounter(
            Duration window,
            Duration bucket,
            Map<Bucket, Status> counterByBucket,
            long createdTs
    ) {
        public record Bucket(
                long from,
                long to
        ) {
            public boolean isInBucket(long val) {
                return from <= val && to >= val;
            }
        }

        public record Status(
                AtomicLong counter,
                long createdTs
        ) {
        }

        public static SlidingWindowCounter init(
                Duration windowDuration,
                Duration bucketDuration
        ) {
            return SlidingWindowCounter.init(
                    windowDuration,
                    bucketDuration,
                    System.currentTimeMillis()
            );
        }

        public static SlidingWindowCounter init(
                Duration windowDuration,
                Duration bucketDuration,
                long createdTs
        ) {
            return new SlidingWindowCounter(
                    windowDuration,
                    bucketDuration,
                    initialiseBuckets(
                            windowDuration,
                            bucketDuration,
                            createdTs
                    ),
                    createdTs
            );
        }

        public SlidingWindowCounter update(long count) {
            return update(
                    this,
                    count
            );
        }

        public static SlidingWindowCounter update(
                SlidingWindowCounter windowCounter,
                long count
        ) {
            return SlidingWindowCounter.update(
                    windowCounter,
                    count,
                    System.currentTimeMillis()
            );
        }

        public static SlidingWindowCounter update(
                SlidingWindowCounter windowCounter,
                long count,
                long now
        ) {
            var bucketAbsoluteValue = (now - windowCounter.createdTs) % windowCounter.window.toMillis();

            windowCounter.counterByBucket.entrySet().stream()
                    .filter(entry ->
                            entry.getKey().isInBucket(bucketAbsoluteValue)
                    )
                    .findFirst()
                    .map(entry ->
                            windowCounter.counterByBucket.compute(
                                    entry.getKey(),
                                    (bucket, status) -> {
                                        requireNonNull(status);

                                        if (isValidForUpdate(bucket, status, now)) {
                                            status.counter.addAndGet(count);
                                            return status;
                                        }

                                        // Outdated bucket found create new
                                        return new Status(
                                                new AtomicLong(count),
                                                now
                                        );
                                    })
                    )
                    .orElseThrow();

            return windowCounter;
        }


        public long sumInWindow() {
            return sumInWindow(
                    this,
                    System.currentTimeMillis()
            );
        }

        public static long sumInWindow(
                SlidingWindowCounter windowCounter
        ) {
            return sumInWindow(
                    windowCounter,
                    System.currentTimeMillis()
            );
        }

        public static long sumInWindow(
                SlidingWindowCounter windowCounter,
                long now
        ) {
            return windowCounter.counterByBucket.entrySet().stream()
                    .filter(entry ->
                            isValidForRead(windowCounter.window, entry.getKey(), entry.getValue(), now)
                    )
                    .mapToLong(e -> e.getValue().counter.get())
                    .sum();
        }

        public SlidingWindowCounter reset() {
            return SlidingWindowCounter.reset(this);
        }

        public static SlidingWindowCounter reset(
                SlidingWindowCounter windowCounter
        ) {
            var createdTs = System.currentTimeMillis();

            windowCounter.counterByBucket.keySet()
                    .forEach(key ->
                            windowCounter.counterByBucket.compute(
                                    key,
                                    (bucket, status) ->
                                            new Status(
                                                    new AtomicLong(0L),
                                                    createdTs
                                            )
                            )
                    );

            return windowCounter;
        }

        // True if now is in range of bucket duration
        static boolean isValidForUpdate(Bucket bucket, Status status, long now) {
            var bucketDuration = bucket.to - bucket.from;

            // Status valid time: [startTs, ... now .... , endTs]
            var startTs = status.createdTs;
            var endTs = startTs + bucketDuration;

            return now >= startTs && now <= endTs;
        }

        // True if bucket overlaps window, back in time, from now
        static boolean isValidForRead(
                Duration window,
                Bucket bucket,
                Status status,
                long now
        ) {
            return isOverlap(
                    // A: Sliding window range [now - window.toMillis(), now]
                    now - window.toMillis(),
                    now,
                    // B: Bucket window range: [status.createdTs, status.createdTs + (bucket.to - bucket.from)]
                    status.createdTs,
                    status.createdTs + (bucket.to - bucket.from)
            );
        }

        static boolean isOverlap(long a1, long a2, long b1, long b2) {
            return Math.max(a1, b1) <= Math.min(a2, b2);
        }

        static Map<Bucket, Status> initialiseBuckets(
                Duration windowDuration,
                Duration bucketDuration,
                long createdTs
        ) {
            return PartitionRange.partition(
                            new Bucket(
                                    0,
                                    windowDuration.toMillis()
                            ),
                            0,
                            windowDuration.toMillis(),
                            bucketDuration.toMillis(),
                            (from, to, value) -> new Bucket(from, to)
                    )
                    .stream()
                    .collect(Collectors.toMap(
                                    bucket -> bucket,
                                    bucket ->
                                            new Status(
                                                    new AtomicLong(0L),
                                                    createdTs
                                            ),
                                    (status, status2) -> {
                                        throw new IllegalStateException("No duplicate buckets allowed");
                                    },
                                    ConcurrentHashMap::new
                            )
                    );
        }
    }

    public record CircuitBreaker(
            AtomicReference<State> state,
            SlidingWindowCounter slidingWindow,
            AtomicLong timestampOpen,
            AtomicLong timestampHalfOpen,
            Policy policy
    ) {
        private static final long RESET_VALUE = Long.MAX_VALUE;

        private enum State {
            OPEN, CLOSE, HALF_OPEN
        }

        public record Policy(
                long maxConsecutiveFailures,
                Duration resetTimeout,
                Duration halfOpenTimeout,
                Duration slidingWindow
        ) {
        }

        public static CircuitBreaker create(Policy policy) {
            return new CircuitBreaker(
                    new AtomicReference<>(State.CLOSE),
                    SlidingWindowCounter.init(
                            policy.slidingWindow,
                            Duration.ofMillis(policy.slidingWindow.toMillis() / 10)
                    ),
                    new AtomicLong(RESET_VALUE),
                    new AtomicLong(RESET_VALUE),
                    policy
            );
        }

        public static <T> Either<RuntimeException, T> tryToExecute(
                CircuitBreaker circuitBreaker,
                Supplier<T> supplier
        ) {
            try {

                if (!circuitBreaker.allow()) {
                    return Either.ofLeft(
                            new RuntimeException("Not allowed to execute")
                    );
                }

                var value = supplier.get();
                circuitBreaker.success();

                return Either.ofRight(value);
            } catch (RuntimeException e) {
                circuitBreaker.failure();
                return Either.ofLeft(e);
            }
        }

        public static boolean tryToExecuteBooleanSupplier(
                CircuitBreaker circuitBreaker,
                Supplier<Boolean> supplier
        ) {
            try {
                if (!circuitBreaker.allow()) {
                    return false;
                }

                var isSuccess = supplier.get();

                if (isSuccess) {
                    circuitBreaker.success();
                } else {
                    circuitBreaker.failure();
                }

                return isSuccess;
            } catch (RuntimeException e) {
                circuitBreaker.failure();
                return false;
            }
        }

        public CircuitBreaker success() {
            slidingWindow.reset();

            timestampOpen.set(RESET_VALUE);
            timestampHalfOpen.set(RESET_VALUE);
            state.set(State.CLOSE);

            return this;
        }

        public CircuitBreaker failure() {
            return failure(1);
        }

        public CircuitBreaker failure(long count) {
            slidingWindow.update(count);

            var sumInWindow = slidingWindow.sumInWindow();
            if (
                // if sum of failures in window > maximum allowed failures in window, then trip circuit OPEN
                    sumInWindow > policy.maxConsecutiveFailures
                            ||
                            // the breaker is tripped again into the OPEN state for another full resetTimeout
                            state.get() == State.HALF_OPEN
            ) {
                var previous = state.getAndSet(State.OPEN);
                if (previous != State.OPEN) {
                    // this thread set new state
                    timestampOpen.set(System.currentTimeMillis());
                    timestampHalfOpen.set(RESET_VALUE);
                }
            }

            return this;
        }

        // Mutating update of circuit breaker
        public boolean allow() {
            if (state.get() == State.OPEN) {

                var timeSinceOpened = System.currentTimeMillis() - timestampOpen.get();

                if (timeSinceOpened > policy.resetTimeout.toMillis()) {

                    var previous = state.getAndSet(State.HALF_OPEN);
                    if (previous == State.OPEN) {
                        // This thread set circuit to half open and is allowed through
                        timestampHalfOpen.set(System.currentTimeMillis());
                        return true;
                    }
                }
            } else if (state.get() == State.HALF_OPEN) {

                // check if state has been in half open too long
                var timeSinceHalfOpened = System.currentTimeMillis() - timestampHalfOpen.get();

                if (timeSinceHalfOpened > policy.halfOpenTimeout.toMillis()) {
                    failure(1);
                }
            }

            return state.get() == State.CLOSE;
        }

        // Note: Non-mutating check of circuit breaker
        public boolean isAllowedThrough() {
            if (state.get() == State.OPEN) {
                var timeSinceOpened = System.currentTimeMillis() - timestampOpen.get();
                if (timeSinceOpened > policy.resetTimeout.toMillis()) {
                    return true;
                }
            } else if (state.get() == State.HALF_OPEN) {

                // check if state has been in half open too long
                var timeSinceHalfOpened = System.currentTimeMillis() - timestampHalfOpen.get();

                if (timeSinceHalfOpened > policy.halfOpenTimeout.toMillis()) {
                    return true;
                }
            }

            return state.get() == State.CLOSE;
        }


        public boolean isOpen() {
            return state.get() == State.OPEN;
        }

        public boolean isHalfOpen() {
            return state.get() == State.HALF_OPEN;
        }

        public boolean isClosed() {
            return state.get() == State.CLOSE;
        }
    }


    public record RateAdjuster(
            AtomicReference<Status> status,
            SlidingWindowCounter slidingWindow,
            Policy policy
    ) {
        public record Status(
                int rate,
                long currentNumSuccesses
        ) {
        }

        public record Policy(
                int initialRate,
                int maxRate,
                int concurrencyIncreaseStep,
                int concurrencyReduceStep,
                int minConsecutiveSuccesses,
                Duration adjustWindow
        ) {
        }

        public static Policy toPolicy(
                int initialRate,
                int maxRate,
                int concurrencyIncreaseStep,
                int concurrencyReduceStep,
                int minConsecutiveSuccesses,
                Duration adjustWindow
        ) {
            return new Policy(
                    Math.max(1, Math.min(initialRate, maxRate)),
                    Math.max(1, Math.max(initialRate, maxRate)),
                    Math.max(1, Math.min(maxRate, concurrencyIncreaseStep)),
                    Math.max(1, Math.min(maxRate, concurrencyReduceStep)),
                    minConsecutiveSuccesses,
                    adjustWindow
            );
        }

        public static RateAdjuster create(Policy policy) {
            return new RateAdjuster(
                    new AtomicReference<>(
                            new Status(
                                    policy.initialRate,
                                    0
                            )
                    ),
                    SlidingWindowCounter.init(
                            policy.adjustWindow,
                            Duration.ofMillis(policy.adjustWindow.toMillis() / 4)
                    ),
                    policy
            );
        }

        public RateAdjuster success() {
            slidingWindow.update(1);

            return this;
        }

        public RateAdjuster failure() {
            slidingWindow.reset();

            status.updateAndGet(
                    existingStatus ->
                            new Status(
                                    Math.max(
                                            policy.initialRate,
                                            existingStatus.rate - policy.concurrencyReduceStep
                                    ),
                                    0
                            )
            );

            return this;
        }

        // NB! calculates and mutates
        public int calculateRate() {
            var numSuccesses = slidingWindow.sumInWindow();
            var existingStatus = status.get();

            if (
                    (numSuccesses - existingStatus.currentNumSuccesses()) >= policy.minConsecutiveSuccesses
            ) {
                // new rate to apply
                var newStatus = new Status(
                        Math.min(policy.maxRate, existingStatus.rate + policy.concurrencyIncreaseStep),
                        numSuccesses
                );

                status.set(newStatus);

                return newStatus.rate;
            }

            return existingStatus.rate;
        }
    }


    public record RateLimiter(
            SlidingWindowCounter slidingWindow,
            Policy policy
    ) {

        public record Policy(
                Duration timebasedFilter,
                long maxNumberToAllow
        ) {
        }

        public static RateLimiter init(
                Duration timebasedFilter,
                long maxNumberToAllow
        ) {
            return new RateLimiter(
                    SlidingWindowCounter.init(
                            timebasedFilter,
                            Duration.ofMillis(timebasedFilter.toMillis() / 4)
                    ),
                    new Policy(
                            timebasedFilter,
                            maxNumberToAllow
                    )
            );
        }

        public static <T> T tryToExecuteOrDefault(
                RateLimiter rateLimiter,
                Supplier<T> supplier,
                T defaultValue
        ) {
            if (!rateLimiter.allow()) {
                return defaultValue;
            }

            return supplier.get();
        }

        public boolean allow() {
            if (slidingWindow.sumInWindow() >= policy.maxNumberToAllow) {
                return false;
            }

            slidingWindow.update(1);
            return true;
        }

        public boolean isAllowed() {
            return slidingWindow.sumInWindow() < policy.maxNumberToAllow;
        }
    }
}
