package org.intact.resilience;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ComputeRunnableTaskTest {
    private static final int THREAD_POOL_SIZE = 8;
    private static final int MAX_CONCURRENCY = THREAD_POOL_SIZE / 2;

    @Test
    public void verify_computeRunnableTask_atMostMaxConcurrencyTasks() {
        var forkJoinPool = new ForkJoinPool(THREAD_POOL_SIZE);

        var submittedLteTasks = Collections.<ForkJoinTask<?>>emptyList();

        for (var i = 0; i < 200; i++) {
            submittedLteTasks =
                    ComputeRunnableTask.Submit.submitRunnableTasks(
                                    new ComputeRunnableTask.Submit.InputDto(
                                            forkJoinPool,
                                            () -> {
                                            },
                                            submittedLteTasks,
                                            MAX_CONCURRENCY
                                    )
                            )
                            .tasksInThreadpool();

            assertTrue(submittedLteTasks.size() <= MAX_CONCURRENCY);
            assertTrue(forkJoinPool.getQueuedTaskCount() <= MAX_CONCURRENCY);
            assertTrue(forkJoinPool.getQueuedSubmissionCount() <= MAX_CONCURRENCY);
        }
    }

    @Test
    public void verify_runWhileWorkHaltsCorrectly() {
        var maxBackoff = Duration.ofSeconds(5);
        var numTimesIsWork = 10;

        var forkJoinPool = new ForkJoinPool(THREAD_POOL_SIZE);

        var checks = new AtomicInteger(0);

        var computed = ComputeRunnableTask.Loop.runWhileWork(
                new ComputeRunnableTask.Loop.InputDto(
                        forkJoinPool,
                        () -> MAX_CONCURRENCY,
                        () -> checks.incrementAndGet() < numTimesIsWork,
                        () -> {
                        },
                        Collections.emptyList(),
                        maxBackoff,
                        1000
                )
        );

        var tasksNotCompleted = ComputeRunnableTask.waitForAll(computed.tasksInThreadPool(), maxBackoff);
        assertTrue(tasksNotCompleted.isEmpty());

        assertEquals(numTimesIsWork, checks.get());
        assertTrue(computed.tasksInThreadPool().size() <= MAX_CONCURRENCY);
        assertTrue(forkJoinPool.getQueuedTaskCount() <= MAX_CONCURRENCY);
        assertTrue(forkJoinPool.getQueuedSubmissionCount() <= MAX_CONCURRENCY);
    }

    @Test
    public void verify_tasksThatHaveWorkAreCalled() {

        var counter1 = new AtomicInteger(0);
        var counter2 = new AtomicInteger(0);
        var counter3 = new AtomicInteger(0);

        var input = new ComputeRunnableTask.Loops.InputDto(
                new ForkJoinPool(THREAD_POOL_SIZE),
                List.of(
                        new ComputeRunnableTask.Loops.InputDto.TaskDto(
                                "Task-1",
                                () -> MAX_CONCURRENCY,
                                () -> true,
                                counter1::incrementAndGet,
                                Collections.emptyList()
                        ),
                        new ComputeRunnableTask.Loops.InputDto.TaskDto(
                                "Task-2",
                                () -> MAX_CONCURRENCY,
                                () -> true,
                                counter2::incrementAndGet,
                                Collections.emptyList()
                        ),
                        new ComputeRunnableTask.Loops.InputDto.TaskDto(
                                "Task-3",
                                () -> MAX_CONCURRENCY,
                                () -> false,
                                counter3::incrementAndGet,
                                Collections.emptyList()
                        )
                ),
                Duration.ofSeconds(1),
                0,
                0
        );

        var computed = ComputeRunnableTask.Loops.runWhileWork(input);

        var tasksNotCompleted = ComputeRunnableTask.waitForAll(
                computed.updatedInput().tasks().stream().flatMap(taskDto -> taskDto.ongoingTasks().stream()).toList(),
                Duration.ofSeconds(10)
        );

        assertTrue(tasksNotCompleted.isEmpty());
        assertTrue(counter1.get() > 0);
        assertTrue(counter2.get() > 0);
        assertEquals(0, counter3.get());
    }

    private record CalculationsDto(
            AtomicInteger counter1,
            AtomicInteger counter2
    ) {
    }

    @Test
    public void verify_circuitBreakerIsObeyed() {

        var maxCounting = 10;
        var maxConsecutiveFailures = 5;

        var calc =
                verify_circuitBreakerIsObeyed(
                        maxCounting,
                        maxConsecutiveFailures,
                        new CalculationsDto(new AtomicInteger(0), new AtomicInteger(0))
                );

        assertEquals(maxCounting, calc.counter1.get());
        assertEquals(maxConsecutiveFailures + 1, calc.counter2.get());
    }

    CalculationsDto verify_circuitBreakerIsObeyed(
            int maxCounting,
            int maxConsecutiveFailures,
            CalculationsDto calc
    ) {
        var circuitBreaker1 = toCircuitBreaker(maxConsecutiveFailures);
        var circuitBreaker2 = toCircuitBreaker(maxConsecutiveFailures);

        var input = new ComputeRunnableTask.Loops.InputDto(
                new ForkJoinPool(THREAD_POOL_SIZE),
                List.of(
                        new ComputeRunnableTask.Loops.InputDto.TaskDto(
                                "Task-1",
                                () -> 1,
                                () ->
                                        calc.counter1.get() < maxCounting && circuitBreaker1.allow(),
                                () -> {
                                    circuitBreaker1.success();
                                    calc.counter1.incrementAndGet();
                                },
                                Collections.emptyList()
                        ),
                        new ComputeRunnableTask.Loops.InputDto.TaskDto(
                                "Task-2",
                                () -> 1,
                                () ->
                                        calc.counter2.get() < maxCounting && circuitBreaker2.allow(),
                                () -> {
                                    circuitBreaker2.failure();
                                    calc.counter2.incrementAndGet();
                                },
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

        return calc;
    }

    private static Resilience.CircuitBreaker toCircuitBreaker(int maxConsecutiveFailures) {
        return Resilience.CircuitBreaker.create(
                new Resilience.CircuitBreaker.Policy(
                        maxConsecutiveFailures,
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(1)
                )
        );
    }
}
