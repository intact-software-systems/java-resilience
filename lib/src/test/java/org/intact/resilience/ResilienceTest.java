package org.intact.resilience;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResilienceTest {

    @Test
    void testSlidingWindowCounter() {

        var windowDuration = Duration.ofMinutes(10);
        var bucketDuration = Duration.ofMinutes(1);
        var now = System.currentTimeMillis();

        var counter = Resilience.SlidingWindowCounter.init(
                windowDuration,
                bucketDuration,
                now
        );

        {
            var expectedNumBuckets = (int) windowDuration.toMinutes() / bucketDuration.toMinutes();

            assertEquals(expectedNumBuckets, counter.counterByBucket().size());
        }

        Resilience.SlidingWindowCounter.update(counter, 1, now);

        var offset = now + Duration.ofMinutes(5).toMillis();

        IntStream.range(0, 100).boxed()
                .forEach(integer ->
                        Resilience.SlidingWindowCounter.update(
                                counter,
                                1,
                                offset
                        )
                );

        {
            var sum = Resilience.SlidingWindowCounter.sumInWindow(counter, offset);

            assertEquals(101, sum);
        }
    }

    @Test
    void testCircuitBreaker() throws InterruptedException {
        var circuitBreaker = Resilience.CircuitBreaker.create(
                new Resilience.CircuitBreaker.Policy(
                        10,
                        Duration.ofMillis(100),
                        Duration.ofMillis(300),
                        Duration.ofMinutes(5)
                )
        );

        assertTrue(circuitBreaker.isClosed());
        assertTrue(circuitBreaker.allow());

        circuitBreaker.success();

        assertTrue(circuitBreaker.isClosed());
        assertTrue(circuitBreaker.allow());

        IntStream.range(0, 11).boxed()
                .forEach(integer -> {
                    circuitBreaker.failure(1);

                    if (integer < 10) {
                        assertTrue(circuitBreaker.isClosed());
                    } else {
                        assertTrue(circuitBreaker.isOpen());
                    }
                });

        assertTrue(circuitBreaker.isOpen());

        Thread.sleep(101);

        assertTrue(circuitBreaker.allow());
        assertTrue(circuitBreaker.isHalfOpen());

        circuitBreaker.success();

        assertTrue(circuitBreaker.isClosed());
        assertTrue(circuitBreaker.allow());
    }

    @Test
    void testCircuitBreakerHalfOpenTimeout() throws InterruptedException {
        var resetTimeout = Duration.ofMillis(10);
        var halfOpenTimeout = Duration.ofMillis(100);
        var maxConsecutiveFailures = 5;

        var circuitBreaker = Resilience.CircuitBreaker.create(
                new Resilience.CircuitBreaker.Policy(
                        maxConsecutiveFailures,
                        resetTimeout,
                        halfOpenTimeout,
                        Duration.ofMinutes(maxConsecutiveFailures)
                )
        );

        assertTrue(circuitBreaker.isClosed());
        assertTrue(circuitBreaker.isAllowedThrough());

        IntStream.range(0, maxConsecutiveFailures + 1).boxed()
                .forEach(integer -> {

                    var either = Resilience.CircuitBreaker.tryToExecute(
                            circuitBreaker,
                            () -> {
                                throw new RuntimeException("failure");
                            }
                    );

                    if (integer < maxConsecutiveFailures) {
                        // Circuit closed

                        assertFalse(either.right.isPresent());
                        assertTrue(either.left.isPresent());
                        assertTrue(circuitBreaker.isClosed());

                    } else {
                        // Circuit open

                        assertFalse(either.right.isPresent());
                        assertTrue(either.left.isPresent());
                        assertTrue(circuitBreaker.isOpen());
                    }
                });

        assertTrue(circuitBreaker.isOpen());

        Thread.sleep(resetTimeout.toMillis() + 1);

        // Note: not calling success/failure after allow causes half open timeout
        {
            assertTrue(circuitBreaker.isAllowedThrough());
            assertTrue(circuitBreaker.allow());
            assertTrue(circuitBreaker.isHalfOpen());
        }

        Thread.sleep(halfOpenTimeout.toMillis() + 1); // half open timeout

        // half open timeout
        {
            // half open timeout and is allowed through (non-mutating)
            assertTrue(circuitBreaker.isAllowedThrough());

            // half open timeout detected which causes allow to return false and either with exception (mutating)
            var either = Resilience.CircuitBreaker.tryToExecute(
                    circuitBreaker,
                    () -> "success"
            );

            assertFalse(either.right.isPresent());
            assertTrue(either.left.isPresent());
            assertTrue(circuitBreaker.isOpen());
        }

        Thread.sleep(resetTimeout.toMillis() + 1);

        // Back to success
        {
            assertTrue(circuitBreaker.isAllowedThrough());

            var either = Resilience.CircuitBreaker.tryToExecute(
                    circuitBreaker,
                    () -> "success"
            );

            assertTrue(either.right.isPresent());
            assertFalse(either.left.isPresent());
            assertTrue(circuitBreaker.isClosed());
        }
    }

    @Test
    void testCircutBreaker_tryToExecuteBooleanSupplier() throws InterruptedException {
        var resetTimeout = Duration.ofMillis(10);
        var halfOpenTimeout = Duration.ofMillis(100);
        var maxConsecutiveFailures = 5;

        var circuitBreaker = Resilience.CircuitBreaker.create(
                new Resilience.CircuitBreaker.Policy(
                        maxConsecutiveFailures,
                        resetTimeout,
                        halfOpenTimeout,
                        Duration.ofMinutes(maxConsecutiveFailures)
                )
        );

        assertTrue(circuitBreaker.isClosed());
        assertTrue(circuitBreaker.isAllowedThrough());

        IntStream.range(0, maxConsecutiveFailures + 1).boxed()
                .forEach(integer -> {

                    var isSuccess = Resilience.CircuitBreaker.tryToExecuteBooleanSupplier(
                            circuitBreaker,
                            () -> false
                    );

                    assertFalse(isSuccess);

                    if (integer < maxConsecutiveFailures) {
                        // Circuit closed
                        assertTrue(circuitBreaker.isClosed());

                    } else {
                        // Circuit open
                        assertTrue(circuitBreaker.isOpen());
                    }
                });

        assertTrue(circuitBreaker.isOpen());

        Thread.sleep(resetTimeout.toMillis() + 1);

        var isSuccess = Resilience.CircuitBreaker.tryToExecuteBooleanSupplier(
                circuitBreaker,
                () -> true
        );

        assertTrue(isSuccess);
        assertTrue(circuitBreaker.isClosed());
    }

    @Test
    void testRateLimiter() {

        var timebasedFilter = Duration.ofMillis(8);

        var rateLimiter = Resilience.RateLimiter.init(
                timebasedFilter,
                1L
        );

        IntStream.range(0, 50).boxed()
                .forEach(i ->
                        {
                            var actual = rateLimiter.slidingWindow().sumInWindow();
                            assertEquals(0, actual);

                            IntStream.range(0, 10).boxed()
                                    .forEach(integer -> {
                                        var allow = rateLimiter.allow();
                                        if (integer < 1) {
                                            assertTrue(allow);
                                        } else {
                                            assertFalse(allow);
                                        }
                                    });

                            sleep(timebasedFilter.toMillis() * 2);
                        }
                );

    }

    @Test
    void testRateAdjuster() {
        var rateAdjuster = Resilience.RateAdjuster.create(
                new Resilience.RateAdjuster.Policy(
                        1,
                        5,
                        1,
                        5,
                        10,
                        Duration.ofSeconds(10)
                )
        );

        IntStream.range(1, 6).boxed()
                .forEach(rate ->
                        IntStream.range(0, 10).boxed()
                                .forEach(integer -> {
                                    var actual = rateAdjuster.calculateRate();
                                    assertEquals(rate, actual);

                                    rateAdjuster.success();
                                })
                );
    }

    @Test
    void testRateAdjuster_decreaseRateUponFailure() {
        var initialRate = 3;
        var maxRate = 6;

        var rateAdjuster = Resilience.RateAdjuster.create(
                new Resilience.RateAdjuster.Policy(
                        initialRate,
                        maxRate,
                        3,
                        1,
                        10,
                        Duration.ofSeconds(60)
                )
        );

        IntStream.range(0, 60).boxed()
                .forEach(integer -> {
                    rateAdjuster.success();

                    var newRate = rateAdjuster.calculateRate(); // mutates rate

                    if(integer >= 10) {
                        assertEquals(maxRate, newRate);
                    }
                });

        assertEquals(rateAdjuster.calculateRate(), maxRate);

        IntStream.range(1, 10).boxed()
                .forEach(integer -> {
                    rateAdjuster.failure();

                    if(integer < 4) {
                        assertEquals(maxRate - integer, rateAdjuster.calculateRate());
                    }
                    else {
                        assertEquals(initialRate, rateAdjuster.calculateRate());
                    }
                });
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}