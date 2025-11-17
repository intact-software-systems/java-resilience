package org.intact.resilience;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public final class BackoffUtil {
    private BackoffUtil() {
    }

    private static final Long MAX_SECONDS = Long.MAX_VALUE / 60;
    private static final Long MAX_HOURS = Long.MAX_VALUE / 3600;
    private static final Long MAX_DAYS = Long.MAX_VALUE / 86400;

    public static long toExponentialBackoff(long attempts) {
        return attempts <= 1
                ? attempts == 0 ? 1 : 2
                : Double.valueOf(
                        Math.pow(
                                2,
                                attempts
                        )
                )
                .longValue();
    }

    public static Duration toExponentialBackoff(TimeUnit timeUnit, long attempts) {
        return switch (timeUnit) {
            case NANOSECONDS -> Duration.ofNanos(toExponentialBackoff(attempts));
            case MICROSECONDS, MILLISECONDS -> Duration.ofMillis(toExponentialBackoff(attempts));
            case SECONDS -> Duration.ofSeconds(toExponentialBackoff(attempts));
            case MINUTES -> Duration.ofMinutes(Math.min(MAX_SECONDS, toExponentialBackoff(attempts)));
            case HOURS -> Duration.ofHours(Math.min(MAX_HOURS, toExponentialBackoff(attempts)));
            case DAYS -> Duration.ofDays(Math.min(MAX_DAYS, toExponentialBackoff(attempts)));
        };
    }

    public static Duration toExponentialBackoff(
            TimeUnit timeUnit,
            long attempts,
            Duration maxBackoff
    ) {
        var backoff = toExponentialBackoff(timeUnit, attempts);

        return maxBackoff.toSeconds() < backoff.toSeconds()
                ? maxBackoff
                : backoff;
    }
}
