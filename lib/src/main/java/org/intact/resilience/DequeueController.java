package org.intact.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DequeueController<K, V, T> {
    public static final int DEFAULT_MAX_NUM_TO_DEQUEUE = 1000;
    public static final int DEFAULT_MAX_NUM_TO_RESERVE = 1;

    private static final Logger log = LoggerFactory.getLogger(DequeueController.class);

    private static final int MAX_CONSECUTIVE_FAILURE_RETRY = 5;
    private static final int MAX_NO_PROGRESSION = 5;
    private static final boolean DEFAULT_RETURN_DEQUEUED_ENTRIES = false;

    boolean returnDequeuedEntries = DEFAULT_RETURN_DEQUEUED_ENTRIES;
    int maxNumDequeue = DEFAULT_MAX_NUM_TO_DEQUEUE;
    Supplier<Integer> maxNumToReserve = () -> DEFAULT_MAX_NUM_TO_RESERVE;
    Supplier<Set<String>> typesToDequeue = Collections::emptySet;
    Function<Set<String>, Boolean> checkIsTypesToDequeue = types -> true;

    BiFunction<Set<String>, Integer, Map<K, V>> newReservator = null;
    Optional<BiFunction<Set<String>, Integer, Map<K, V>>> retryReservator = Optional.empty();
    Optional<BiFunction<Set<String>, Integer, Map<K, V>>> failedReservator = Optional.empty();
    Optional<BiFunction<Set<String>, Integer, Map<K, V>>> timeoutReservator = Optional.empty();

    Function<Map<K, SuccessDto<K, V, T>>, Map<K, SuccessDto<K, V, T>>> successReleaser = null;
    Function<Map<K, FailureDto<K, V>>, Map<K, FailureDto<K, V>>> failureReleaser = null;

    Optional<Consumer<Map<K, SuccessDto<K, V, T>>>> onCompleted = Optional.empty();
    Optional<Consumer<Map<K, FailureDto<K, V>>>> onFailed = Optional.empty();
    Optional<Function<Map<K, V>, Map<K, V>>> onPreProcessingReservedEntries = Optional.empty();

    public static <K, V, T> DequeueController<K, V, T> create() {
        return new DequeueController<>();
    }

    public DequeueController<K, V, T> withMaxNumToDequeue(int maxNumDequeue) {
        if (maxNumDequeue <= 0) {
            throw new RuntimeException("Illegal maxNumDequeue: " + maxNumDequeue + ". Must be positive.");
        }

        this.maxNumDequeue = maxNumDequeue;
        return this;
    }

    public DequeueController<K, V, T> withMaxNumToReserve(Supplier<Integer> maxNumToReserve) {
        this.maxNumToReserve = maxNumToReserve;
        return this;
    }

    public DequeueController<K, V, T> withInboxTypesToDequeue(Supplier<Set<String>> typesToDequeue) {
        this.typesToDequeue = requireNonNull(typesToDequeue);
        return this;
    }

    public DequeueController<K, V, T> withReturnDequeuedEntries(boolean returnDequeuedEntries) {
        this.returnDequeuedEntries = returnDequeuedEntries;
        return this;
    }

    public DequeueController<K, V, T> onCheckIsTypesToDequeueDo(Function<Set<String>, Boolean> checkIsTypesToDequeue) {
        this.checkIsTypesToDequeue = requireNonNull(checkIsTypesToDequeue);
        return this;
    }

    public DequeueController<K, V, T> onNewEntriesReserveDo(BiFunction<Set<String>, Integer, Map<K, V>> reservator) {
        this.newReservator = requireNonNull(reservator);
        return this;
    }

    public DequeueController<K, V, T> onRetryEntriesReserveDo(BiFunction<Set<String>, Integer, Map<K, V>> retryReservator) {
        this.retryReservator = Optional.ofNullable(retryReservator);
        return this;
    }

    public DequeueController<K, V, T> onFailedEntriesReserveDo(BiFunction<Set<String>, Integer, Map<K, V>> failedReservator) {
        this.failedReservator = Optional.ofNullable(failedReservator);
        return this;
    }

    public DequeueController<K, V, T> onTimeoutEntriesReserveDo(BiFunction<Set<String>, Integer, Map<K, V>> timeoutReservator) {
        this.timeoutReservator = Optional.ofNullable(timeoutReservator);
        return this;
    }

    public DequeueController<K, V, T> onReleaseEntriesDo(
            Function<Map<K, SuccessDto<K, V, T>>, Map<K, SuccessDto<K, V, T>>> successReleaser,
            Function<Map<K, FailureDto<K, V>>, Map<K, FailureDto<K, V>>> failureReleaser
    ) {
        this.successReleaser = requireNonNull(successReleaser);
        this.failureReleaser = requireNonNull(failureReleaser);
        return this;
    }

    public DequeueController<K, V, T> onCompletedEntries(
            Consumer<Map<K, SuccessDto<K, V, T>>> onCompleted
    ) {
        this.onCompleted = Optional.ofNullable(onCompleted);
        return this;
    }

    public DequeueController<K, V, T> onFailedEntries(
            Consumer<Map<K, FailureDto<K, V>>> onFailed
    ) {
        this.onFailed = Optional.ofNullable(onFailed);
        return this;
    }

    @SuppressWarnings("unused")
    public DequeueController<K, V, T> onPreProcessingReservedEntries(
            Function<Map<K, V>, Map<K, V>> onPreProcessingReservedEntries
    ) {
        this.onPreProcessingReservedEntries = Optional.ofNullable(onPreProcessingReservedEntries);
        return this;
    }

    public enum Reservator {NEW, RETRY, FAILED, TIMEOUT}

    public Map<Reservator, Map<K, Either<FailureDto<K, V>, SuccessDto<K, V, T>>>> dequeueForCompute(
            BiFunction<K, V, T> computer
    ) {
        if (typesToDequeue.get().isEmpty()) {
            log.warn("No types provided for callbacks: [newReservator, retryReservator, failedReservator].");
        }

        if (this.retryReservator.isEmpty()) {
            log.warn("No retry reservator configured. Retries are not performed.");
        }

        if (this.failedReservator.isEmpty()) {
            log.warn("No failed reservator configured. Retries on failed resources are not performed.");
        }

        requireNonNull(computer);
        requireNonNull(this.newReservator);
        requireNonNull(this.successReleaser);
        requireNonNull(this.failureReleaser);

        return Map.of(

                // Get queued items using newReservator
                Reservator.NEW,
                dequeueWithTypes(
                        this,
                        newReservator,
                        computer,
                        this.maxNumDequeue
                ),

                // Get queued items using retryReservator
                Reservator.RETRY,
                this.retryReservator
                        .map(reservator ->
                                dequeueWithTypes(
                                        this,
                                        reservator,
                                        computer,
                                        this.maxNumDequeue
                                )
                        )
                        .orElseGet(Collections::emptyMap),

                // Get queued items using failedReservator
                Reservator.FAILED,
                this.failedReservator
                        .map(reservator ->
                                dequeueWithTypes(
                                        this,
                                        reservator,
                                        computer,
                                        this.maxNumDequeue
                                )
                        )
                        .orElseGet(Collections::emptyMap),

                // Get queued items using timeoutReservator
                Reservator.TIMEOUT,
                this.timeoutReservator
                        .map(reservator ->
                                dequeueWithTypes(
                                        this,
                                        reservator,
                                        computer,
                                        this.maxNumDequeue
                                )
                        )
                        .orElseGet(Collections::emptyMap)
        );
    }

    public record FailureDto<K, V>(
            K key,
            V value,
            RuntimeException exception
    ) {
    }

    public record SuccessDto<K, V, T>(
            K key,
            V value,
            T computedValue
    ) {
    }

    public static <K, V, T> Map<K, Either<FailureDto<K, V>, SuccessDto<K, V, T>>> dequeueWithTypes(
            DequeueController<K, V, T> dequeue,
            BiFunction<Set<String>, Integer, Map<K, V>> reservator,
            BiFunction<K, V, T> computer,
            int maxNumDequeue
    ) {
        return dequeueWithTypes(
                maxNumDequeue,
                dequeue.maxNumToReserve,
                dequeue.typesToDequeue,
                dequeue.returnDequeuedEntries,
                dequeue.checkIsTypesToDequeue,
                reservator,
                computer,
                dequeue.successReleaser,
                dequeue.failureReleaser,
                dequeue.onCompleted,
                dequeue.onFailed,
                dequeue.onPreProcessingReservedEntries
        );
    }

    public static <K, V, T> Map<K, Either<FailureDto<K, V>, SuccessDto<K, V, T>>> dequeueWithTypes(
            int maxNumDequeue,
            Supplier<Integer> maxNumToReserve,
            Supplier<Set<String>> typesToDequeue,
            boolean returnDequeuedEntries,
            Function<Set<String>, Boolean> isWorkForTypes,
            BiFunction<Set<String>, Integer, Map<K, V>> reservator,
            BiFunction<K, V, T> computer,
            Function<Map<K, SuccessDto<K, V, T>>, Map<K, SuccessDto<K, V, T>>> successReleaser,
            Function<Map<K, FailureDto<K, V>>, Map<K, FailureDto<K, V>>> failureReleaser,
            Optional<Consumer<Map<K, SuccessDto<K, V, T>>>> onCompleted,
            Optional<Consumer<Map<K, FailureDto<K, V>>>> onFailed,
            Optional<Function<Map<K, V>, Map<K, V>>> onPreProcessingReservedEntries
    ) {
        long consecutiveFailureCounter = 0;
        long nonProgressionCounter = 0;
        long dequeuedCounter = 0;

        Map<K, Either<FailureDto<K, V>, SuccessDto<K, V, T>>> allComputed = new HashMap<>();

        while (isWorkForTypes.apply(typesToDequeue.get()) && consecutiveFailureCounter < MAX_CONSECUTIVE_FAILURE_RETRY) {

            try {
                var numToReserve = maxNumToReserve.get();
                if (numToReserve <= 0) {
                    log.debug("Number to reserve for {} was {}. Returning.", typesToDequeue.get(), numToReserve);
                    return allComputed;
                }

                Map<K, Either<FailureDto<K, V>, SuccessDto<K, V, T>>> computed =
                        Optional.of(
                                        reservator.apply(typesToDequeue.get(), numToReserve)
                                )
                                .map(reserved ->
                                        onPreProcessingReservedEntries
                                                .map(f -> f.apply(reserved))
                                                .orElse(reserved)
                                )
                                .orElse(Collections.emptyMap()).entrySet().stream()
                                .collect(Collectors.toMap(
                                                Map.Entry::getKey,
                                                e -> {
                                                    try {
                                                        var computedValue = computer.apply(e.getKey(), e.getValue());

                                                        return Either
                                                                .ofRight(
                                                                        new SuccessDto<>(
                                                                                e.getKey(),
                                                                                e.getValue(),
                                                                                computedValue
                                                                        )
                                                                );
                                                    } catch (RuntimeException exception) {
                                                        log.error("Computer failed on dequeued key {}. ", e.getKey(), exception);
                                                        return Either
                                                                .ofLeft(
                                                                        new FailureDto<>(e.getKey(), e.getValue(), exception)
                                                                );
                                                    }
                                                }
                                        )
                                );

                var folded = Either.EitherCollectors.toMapFoldBoth(computed);

                var successByKey = folded.rightByKey();
                var failureByKey = folded.leftByKey();

                // success handling
                {
                    if (!successByKey.isEmpty()) {
                        successReleaser.apply(
                                        successByKey
                                )
                                .forEach(
                                        (k, successDto) -> computed.replace(k, Either.ofRight(successDto))
                                );
                        nonProgressionCounter = 0;
                    } else {
                        ++nonProgressionCounter;
                    }

                    if (!successByKey.isEmpty() || failureByKey.isEmpty()) {
                        onCompleted.ifPresent(
                                onCompletedFunction -> onCompletedFunction.accept(Either.EitherCollectors.toMapFoldRights(computed))
                        );
                    }
                }

                // failure handling
                {
                    if (!failureByKey.isEmpty()) {
                        failureReleaser.apply(
                                        failureByKey
                                )
                                .forEach(
                                        (k, failureDto) -> computed.replace(k, Either.ofLeft(failureDto))
                                );

                        onFailed.ifPresent(
                                onFailedFunction -> onFailedFunction.accept(Either.EitherCollectors.toMapFoldLefts(computed))
                        );

                        nonProgressionCounter = 0;
                    }
                }

                if (returnDequeuedEntries) {
                    allComputed.putAll(computed);
                }

                // If reservator returns same elements over and over, count them
                {
                    dequeuedCounter += computed.size();

                    if (dequeuedCounter >= maxNumDequeue) {
                        log.debug("Dequeued {} entries, dequeuedCounter {} >= maxNumDequeue {}. Returning ...", allComputed.size(), dequeuedCounter, maxNumDequeue);
                        break;
                    }
                }

                if (nonProgressionCounter > MAX_NO_PROGRESSION) {
                    log.warn("No progression (no success or failures) last {} times. Returning quietly.", nonProgressionCounter);
                    break;
                }

                consecutiveFailureCounter = 0;

                if (computed.isEmpty()) {
                    log.debug("No more work to do on {}. Returning.", typesToDequeue.get());
                    return allComputed;
                }

            } catch (RuntimeException e) {
                log.error("Exception caught while working on queue with these types: {}", typesToDequeue.get(), e);
                ++consecutiveFailureCounter;

                if (!backoffWithSleep(consecutiveFailureCounter)) {
                    log.warn("Backoff with sleep interrupted, returning quietly.");
                    break;
                }
            }
        }

        if (consecutiveFailureCounter >= MAX_CONSECUTIVE_FAILURE_RETRY) {
            log.warn("Dequeue experienced {} failures in a row. Giving up ... ", consecutiveFailureCounter);
        }

        return allComputed;
    }

    static boolean backoffWithSleep(long failureCounter) {
        try {
            var sleepDuration = BackoffUtil.toExponentialBackoff(TimeUnit.SECONDS, failureCounter);
            log.warn("{} attempt. Backoff sleeping {} secs", failureCounter, sleepDuration.toSeconds());

            TimeUnit.SECONDS.sleep(sleepDuration.toSeconds());
            return true;
        } catch (Exception e) {
            log.warn("Exception caught in backoff", e);
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
