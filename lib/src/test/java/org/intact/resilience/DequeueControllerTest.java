package org.intact.resilience;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.intact.resilience.DequeueController.Reservator.FAILED;
import static org.intact.resilience.DequeueController.Reservator.NEW;
import static org.intact.resilience.DequeueController.Reservator.RETRY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DequeueControllerTest {

    private static final String NEWS_TYPE = "NEWS";

    private static final Set<String> TYPES_TO_DEQUEUE = Set.of(NEWS_TYPE);

    private static final int START_INCLUSIVE = 1;
    private static final int END_EXCLUSIVE = 100;

    record Key(
            String typeDequeued,
            String resourceId
    ) {
    }

    record Job(
            Key key,
            String workUrl
    ) {
    }

    record Computed(
            String value
    ) {
    }

    public static final Map<Key, Job> db = initDb();


    public static Map<Key, Job> initDb() {
        return IntStream.range(START_INCLUSIVE, END_EXCLUSIVE)
                .mapToObj(value -> new Job(new Key(NEWS_TYPE, Integer.toString(value)), "http://www.vg.no"))
                .collect(Collectors.toMap(Job::key, j -> j));
    }

    @Test
    public void inboxQueueDequeueSuccessTest() {
        var reserve = new AtomicInteger(START_INCLUSIVE);
        var called = new HashMap<Key, Job>();

        var completed = new AtomicReference<Map<Key, DequeueController.SuccessDto<Key, Job, Computed>>>(emptyMap());
        var failed = new AtomicReference<Map<Key, DequeueController.FailureDto<Key, Job>>>(emptyMap());

        var dequeued = DequeueController.<Key, Job, Computed>create()
                .withInboxTypesToDequeue(() -> TYPES_TO_DEQUEUE)
                .withMaxNumToDequeue(100)
                .withReturnDequeuedEntries(true)
                .onCheckIsTypesToDequeueDo(typesToDequeue -> reserve.get() < END_EXCLUSIVE)
                .onNewEntriesReserveDo((typesToDequeue, maxNumToReserve) ->
                        reserveOneJob(reserve)
                )
                .onReleaseEntriesDo(
                        successByKey -> Collections.emptyMap(),
                        failureByKey -> Collections.emptyMap()
                )
                .onCompletedEntries(completed::set)
                .onFailedEntries(failed::set)
                .dequeueForCompute(
                        (key, job) ->
                        {
                            called.put(key, job);

                            return new Computed(job.workUrl);
                        }
                );

        db.forEach((key, job) -> {
            assertTrue(called.containsKey(key));
            assertTrue(dequeued.get(NEW).containsKey(key));
        });

        assertFalse(dequeued.get(NEW).isEmpty());
        assertTrue(dequeued.get(FAILED).isEmpty());
        assertTrue(dequeued.get(RETRY).isEmpty());

        assertFalse(completed.get().isEmpty());
        assertTrue(failed.get().isEmpty());
    }

    @Test
    public void inboxQueueReserveAllNoTypesSuccessTest() {
        var reserve = new AtomicInteger(START_INCLUSIVE);
        var called = new HashMap<Key, Job>();

        var dequeued = DequeueController.<Key, Job, Computed>create()
                .withReturnDequeuedEntries(true)
                .onCheckIsTypesToDequeueDo(typesToDequeue ->
                        {
                            assertTrue(typesToDequeue.isEmpty());
                            return reserve.get() < END_EXCLUSIVE;
                        }
                )
                .onNewEntriesReserveDo((typesToDequeue, maxNumToReserve) ->
                        {
                            assertTrue(typesToDequeue.isEmpty());

                            // reserve all
                            reserve.set(END_EXCLUSIVE);
                            return db.entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        }
                )
                .onReleaseEntriesDo(
                        successByKey -> Collections.emptyMap(),
                        failureByKey -> Collections.emptyMap()
                )
                .dequeueForCompute((key, job) -> {
                    called.put(key, job);

                    return new Computed(
                            job.workUrl
                    );
                });

        db.forEach((key, job) -> {
            assertTrue(called.containsKey(key));
            assertTrue(dequeued.get(NEW).containsKey(key));
        });

        assertTrue(dequeued.get(FAILED).isEmpty());
        assertTrue(dequeued.get(RETRY).isEmpty());
    }

    @Test
    public void inboxQueueControllerWithFailuresTest() {
        var reserve = new AtomicInteger(START_INCLUSIVE);
        var completed = new AtomicReference<Map<Key, DequeueController.SuccessDto<Key, Job, Boolean>>>(emptyMap());
        var failed = new AtomicReference<Map<Key, DequeueController.FailureDto<Key, Job>>>(emptyMap());

        var resourceToFail = "10";
        var failureHandlerCalled = new AtomicBoolean(false);

        var called = new HashMap<Key, Job>();

        var dequeued = DequeueController.<Key, Job, Boolean>create()
                .withInboxTypesToDequeue(() -> TYPES_TO_DEQUEUE)
                .withMaxNumToDequeue(100)
                .withReturnDequeuedEntries(true)
                .onCheckIsTypesToDequeueDo(typesToDequeue -> reserve.get() < END_EXCLUSIVE)
                .onNewEntriesReserveDo((typesToDequeue, maxNumToReserve) ->
                        reserveOneJob(reserve)
                )
                .onReleaseEntriesDo(
                        successByKey -> {
                            successByKey
                                    .forEach((key1, keyJobFailureDto) ->
                                            assertNotEquals(resourceToFail, key1.resourceId)
                                    );
                            return Collections.emptyMap();
                        },
                        failureByKey -> {
                            failureByKey
                                    .forEach((key1, keyJobFailureDto) ->
                                            assertEquals(resourceToFail, key1.resourceId)
                                    );

                            failureHandlerCalled.set(true);
                            return Collections.emptyMap();
                        }
                )
                .onCompletedEntries(completed::set)
                .onFailedEntries(failed::set)
                .dequeueForCompute((key, job) -> {
                    if (key.resourceId.equals(resourceToFail)) {
                        throw new RuntimeException("Failed to compute resource " + resourceToFail);
                    }

                    called.put(key, job);
                    return true;
                });


        assertTrue(failureHandlerCalled.get());

        db.forEach((key, job) -> {
            if (key.resourceId.equals(resourceToFail)) {
                assertFalse(called.containsKey(key));

                assertTrue(dequeued.get(NEW).containsKey(key));
                assertTrue(dequeued.get(NEW).get(key).left.isPresent());
                assertFalse(dequeued.get(NEW).get(key).right.isPresent());

            } else {
                assertTrue(called.containsKey(key));
                assertTrue(dequeued.get(NEW).containsKey(key));
            }
        });

        assertTrue(dequeued.get(FAILED).isEmpty());
        assertTrue(dequeued.get(RETRY).isEmpty());

        assertFalse(completed.get().isEmpty());

        // Not empty due to early failure. Retry occurred that corrected the problem
        assertFalse(failed.get().isEmpty());
    }

    @Test
    public void inboxQueueNoneReservedHalts() {
        var reserve = new AtomicInteger(START_INCLUSIVE);

        var called = new HashMap<Key, Job>();

        var dequeued = DequeueController.<Key, Job, Boolean>create()
                .withInboxTypesToDequeue(() -> TYPES_TO_DEQUEUE)
                .withReturnDequeuedEntries(true)
                .onCheckIsTypesToDequeueDo(typesToDequeue -> reserve.get() < END_EXCLUSIVE)
                .onNewEntriesReserveDo((typesToDequeue, maxNumToReserve) ->
                        Collections.emptyMap() // none reserved
                )
                .onReleaseEntriesDo(
                        successByKey -> Collections.emptyMap(),
                        failureByKey -> Collections.emptyMap()
                )
                .dequeueForCompute((key, job) -> {
                    called.put(key, job);
                    return true;
                });

        assertTrue(called.isEmpty());
        assertTrue(dequeued.get(NEW).isEmpty());
        assertTrue(dequeued.get(FAILED).isEmpty());
        assertTrue(dequeued.get(RETRY).isEmpty());
    }


    @RepeatedTest(10)
    public void inboxDequeueWithCircuitBreaker() {
        var reserve = new AtomicInteger(START_INCLUSIVE);
        var maxConsecutiveFailures = 10;

        var circuitBreaker = Resilience.CircuitBreaker.create(
                new Resilience.CircuitBreaker.Policy(
                        maxConsecutiveFailures,
                        Duration.ofMillis(1),
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(1)
                )
        );

        var dequeued = DequeueController.<Key, Job, Boolean>create()
                .withInboxTypesToDequeue(() -> TYPES_TO_DEQUEUE)
                .withMaxNumToDequeue(100)
                .withReturnDequeuedEntries(true)
                .withMaxNumToReserve(
                        () -> circuitBreaker.allow()
                                ? 1
                                : 0
                )
                .onCheckIsTypesToDequeueDo(typesToDequeue -> reserve.get() < END_EXCLUSIVE)
                .onNewEntriesReserveDo((typesToDequeue, maxNumToReserve) ->
                        reserveOneJob(reserve)
                )
                .onReleaseEntriesDo(
                        successByKey -> Collections.emptyMap(),
                        failureByKey -> Collections.emptyMap()
                )
                .onCompletedEntries(successes ->
                        circuitBreaker.success()
                )
                .onFailedEntries(failures ->
                        circuitBreaker.failure()
                )
                .dequeueForCompute((key, job) -> {
                    throw new RuntimeException("Failed to compute resource " + key);
                });

        assertTrue(circuitBreaker.isOpen());

        assertEquals(START_INCLUSIVE + maxConsecutiveFailures + 1, reserve.get());

        assertEquals(11, Either.EitherCollectors.toMapFoldLefts(dequeued.get(NEW)).size());
        assertEquals(0, Either.EitherCollectors.toMapFoldRights(dequeued.get(NEW)).size());

        assertTrue(dequeued.get(FAILED).isEmpty());
        assertTrue(dequeued.get(RETRY).isEmpty());
    }


    @RepeatedTest(10)
    public void inboxDequeueInParallelWithCircuitBreaker() {
        var reserve = new AtomicInteger(START_INCLUSIVE);
        var maxConsecutiveFailures = 10;
        var parallelism = 4;

        var circuitBreaker = Resilience.CircuitBreaker.create(
                new Resilience.CircuitBreaker.Policy(
                        maxConsecutiveFailures,
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(1)
                )
        );


        var input = new ComputeRunnableTask.Loops.InputDto(
                new ForkJoinPool(parallelism),
                List.of(
                        new ComputeRunnableTask.Loops.InputDto.TaskDto(
                                "Task-1",
                                () -> parallelism,
                                circuitBreaker::isAllowedThrough,
                                () ->
                                        DequeueController.<Key, Job, Boolean>create()
                                                .withInboxTypesToDequeue(() -> TYPES_TO_DEQUEUE)
                                                .onCheckIsTypesToDequeueDo(typesToDequeue -> reserve.get() < END_EXCLUSIVE)
                                                .withMaxNumToDequeue(10)
                                                .withMaxNumToReserve(
                                                        () -> circuitBreaker.allow()
                                                                ? 1
                                                                : 0
                                                )
                                                .onNewEntriesReserveDo((typesToDequeue, maxNumToReserve) ->
                                                        reserveOneJob(reserve)
                                                )
                                                .onReleaseEntriesDo(
                                                        successByKey -> Collections.emptyMap(),
                                                        failureByKey -> Collections.emptyMap()
                                                )
                                                .onCompletedEntries(successes ->
                                                        circuitBreaker.success()
                                                )
                                                .onFailedEntries(failures ->
                                                        circuitBreaker.failure()
                                                )
                                                .dequeueForCompute((key, job) -> {
                                                    throw new RuntimeException("Failed to compute resource " + key);
                                                }),
                                Collections.emptyList()
                        )
                ),
                Duration.ofSeconds(1),
                100,
                5
        );

        var computed = ComputeRunnableTask.Loops.runWhileWork(input);

        var tasksNotCompleted = ComputeRunnableTask.waitForAll(
                computed.updatedInput().tasks().stream().flatMap(taskDto -> taskDto.ongoingTasks().stream()).toList(),
                Duration.ofSeconds(5)
        );

        assertTrue(tasksNotCompleted.isEmpty());
        assertTrue(circuitBreaker.isOpen());
    }

    private static Map<Key, Job> reserveOneJob(AtomicInteger reserve) {
        var key = new Key(NEWS_TYPE, Integer.toString(reserve.get()));
        reserve.incrementAndGet();
        return Map.of(key, requireNonNull(db.get(key)));
    }
}