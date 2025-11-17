package org.intact.resilience;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class WaitForCompletion {

    private static final int MAX_TIMES_FAILED_STATUS = 5;

    private static final long WAIT_TIME_MSECS = 100;
    private static final long MAX_WAIT_TIME_MSECS = 1000;

    public record TimeoutDto(
            Duration maxRunTime,
            LocalDateTime startTime,
            int maxTimesFailedStatus,
            long waitTimeMsecs,
            long maxWaitTimeMsecs,
            Supplier<Boolean> isTimeout
    ) {
        public static TimeoutDto toTimeoutDto(
                Duration maxRunTime
        ) {
            return toTimeoutDto(maxRunTime, LocalDateTime.now());
        }

        public static TimeoutDto toTimeoutDto(
                Duration maxRunTime,
                LocalDateTime startTime
        ) {
            return new TimeoutDto(
                    maxRunTime,
                    startTime,
                    MAX_TIMES_FAILED_STATUS,
                    WAIT_TIME_MSECS,
                    MAX_WAIT_TIME_MSECS,
                    () -> Duration.between(startTime, LocalDateTime.now()).toMinutes() > maxRunTime.toMinutes()
            );
        }
    }

    public static boolean waitForCompletion(
            Supplier<Long> pollForCompletion,
            Consumer<Long> onNext,
            TimeoutDto timeout
    ) {
        long failedToWaitCounter = 0;
        long loopCounter = 0;

        var polledJobStatusRef = new AtomicReference<Long>();

        while (!timeout.isTimeout.get()) {
            try {
                polledJobStatusRef.set(pollForCompletion.get());

                var numOfResourcesNotCompleted = polledJobStatusRef.get();

                onNext.accept(numOfResourcesNotCompleted);

                if (numOfResourcesNotCompleted > 0) {
                    waitInMilliSec(Math.min(WAIT_TIME_MSECS * ++loopCounter, MAX_WAIT_TIME_MSECS));
                } else {
                    return true;
                }

            } catch (RuntimeException e) {
                ++failedToWaitCounter;

                if (failedToWaitCounter > MAX_TIMES_FAILED_STATUS) {
                    Optional.ofNullable(polledJobStatusRef.get())
                            .ifPresent(onNext);
                    return false;
                }

                backoffSleep(failedToWaitCounter);
            }
        }

        return false;
    }

    public static <T> T tryNTimes(
            Supplier<T> perform,
            Function<RuntimeException, T> orElse,
            BiConsumer<TimeoutDto, RuntimeException> onError,
            int maxNumRetry,
            Duration maxRunTime
    ) {
        var timeout =
                TimeoutDto
                        .toTimeoutDto(maxRunTime);

        for (int i = 0; i < maxNumRetry; i++) {
            try {
                return perform.get();
            } catch (RuntimeException e) {
                if (timeout.isTimeout.get()) {
                    return orElse.apply(e);
                }

                try {
                    onError.accept(timeout, e);
                } catch (RuntimeException e1) {
                    return orElse.apply(e);
                }

                WaitForCompletion.backoffSleep(i);
            }
        }

        return orElse.apply(
                new IllegalStateException("Failed to execute supplier within " + maxRunTime.toSeconds() + " seconds and retries " + maxNumRetry)
        );
    }

    static void waitInMilliSec(long waitTimeMsecs) {
        try {
            Thread.sleep(waitTimeMsecs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void backoffSleep(long failedToWaitCounter) {
        waitInMilliSec(
                BackoffUtil.toExponentialBackoff(
                                TimeUnit.MILLISECONDS,
                                failedToWaitCounter,
                                Duration.ofMillis(MAX_WAIT_TIME_MSECS)
                        )
                        .toMillis()
        );
    }
}
