package org.intact.resilience;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BackoffUtilTest {
    private static final int START_INCLUSIVE = 0;
    private static final int END_EXCLUSIVE = 10;

    @Test
    void testBackoffSeconds() {
        Duration prevSleep = null;

        for (int i = START_INCLUSIVE; i < END_EXCLUSIVE; i++) {
            var sleep = BackoffUtil.toExponentialBackoff(TimeUnit.SECONDS, i);

            assertBackoff(
                    i,
                    sleep.toSeconds(),
                    Optional.ofNullable(prevSleep)
                            .map(Duration::toSeconds)
                            .orElse(0L)
            );

            if (i > 1) {
                prevSleep = sleep;
            }
        }

        var maxBackoff = Duration.ofMinutes(1);
        var sleep = BackoffUtil.toExponentialBackoff(TimeUnit.SECONDS, 100, maxBackoff);

        assertEquals(maxBackoff.toSeconds(), sleep.toSeconds());
    }

    @Test
    void testBackoffMillis() {
        Duration prevSleep = null;

        for (int i = START_INCLUSIVE; i < END_EXCLUSIVE; i++) {
            var sleep = BackoffUtil.toExponentialBackoff(TimeUnit.MILLISECONDS, i);

            assertBackoff(
                    i,
                    sleep.toMillis(),
                    Optional.ofNullable(prevSleep)
                            .map(Duration::toMillis)
                            .orElse(0L)
            );

            if (i > 1) {
                prevSleep = sleep;
            }
        }

        var maxBackoff = Duration.ofMinutes(1);
        var sleep = BackoffUtil.toExponentialBackoff(TimeUnit.MILLISECONDS, 100, maxBackoff);

        assertEquals(maxBackoff.toMillis(), sleep.toMillis());
    }

    @Test
    void testBackOffLarge() {
        BackoffUtil.toExponentialBackoff(TimeUnit.NANOSECONDS, Long.MAX_VALUE);
        BackoffUtil.toExponentialBackoff(TimeUnit.MICROSECONDS, Long.MAX_VALUE);
        BackoffUtil.toExponentialBackoff(TimeUnit.MILLISECONDS, Long.MAX_VALUE);
        BackoffUtil.toExponentialBackoff(TimeUnit.SECONDS, Long.MAX_VALUE);
        BackoffUtil.toExponentialBackoff(TimeUnit.MINUTES, Long.MAX_VALUE);
        BackoffUtil.toExponentialBackoff(TimeUnit.HOURS, Long.MAX_VALUE);
        BackoffUtil.toExponentialBackoff(TimeUnit.DAYS, Long.MAX_VALUE);
    }

    private static void assertBackoff(
            int i,
            long sleepTime,
            long prevSleepTime
    ) {
        if (i == START_INCLUSIVE) {
            assertEquals(1, sleepTime);
        } else if (i == 1) {
            assertEquals(2, sleepTime);
        } else if (i == 2) {
            assertEquals(4, sleepTime);
        } else {
            assertEquals(prevSleepTime * 2, sleepTime);
        }
    }
}