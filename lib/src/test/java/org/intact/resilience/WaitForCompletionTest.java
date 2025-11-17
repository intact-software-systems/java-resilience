package org.intact.resilience;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WaitForCompletionTest {

    private static final Logger log = LoggerFactory.getLogger(WaitForCompletionTest.class);

    @Test
    public void testWaitForCompletion() {
        AtomicLong counter = new AtomicLong(4);

        assertTrue(
                WaitForCompletion.waitForCompletion(
                        counter::decrementAndGet,
                        numOfResourcesNotCompleted ->
                                log.info("Waiting for {} resources to complete.", numOfResourcesNotCompleted),
                        WaitForCompletion.TimeoutDto.toTimeoutDto(Duration.ofSeconds(2))
                )
        );
    }

    @Test
    public void testTryNTimes() {
        AtomicLong counter = new AtomicLong(0);

        var result =
                WaitForCompletion.tryNTimes(
                        () -> {
                            if (counter.incrementAndGet() < 5) {
                                throw new RuntimeException("test");
                            }
                            return counter.get();
                        },
                        e -> -1,
                        (timeoutDto, e) ->
                                log.warn("Failed to wait ", e),
                        10,
                        Duration.ofSeconds(10)
                );

        assertEquals(5L, result);
    }

    @Test
    public void testTryNTimesWhenFailure() {
        AtomicLong counter = new AtomicLong(0);

        var result =
                WaitForCompletion.tryNTimes(
                        () -> {
                            if (counter.incrementAndGet() < 6) {
                                throw new RuntimeException("test");
                            }
                            return counter.get();
                        },
                        e -> -1L,
                        (timeoutDto, e) ->
                                log.warn("Failed to wait ", e),
                        5,
                        Duration.ofSeconds(10)
                );

        assertEquals(-1L, result);
    }

    @Test
    public void testTryNTimesButNotWhenUnacceptableExceptionFailure() {
        AtomicLong counter = new AtomicLong(0);

        Long result =
                WaitForCompletion.tryNTimes(
                        () -> {
                            if (counter.incrementAndGet() < 6) {
                                throw new IllegalStateException("test");
                            }
                            return counter.get();
                        },
                        e -> -1L,
                        (timeoutDto, e) -> {
                            if (e instanceof IllegalStateException) {
                                throw e;
                            }
                            log.warn("Failed to wait ", e);
                        },
                        5,
                        Duration.ofSeconds(10)
                );

        assertEquals(-1L, result);
    }

}