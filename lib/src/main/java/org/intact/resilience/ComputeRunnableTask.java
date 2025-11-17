package org.intact.resilience;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ComputeRunnableTask {

    public static List<ForkJoinTask<?>> waitForAll(List<ForkJoinTask<?>> tasksInThreadPool, Duration maxWaitTimeForTask) {
        tasksInThreadPool.stream()
                .filter(task -> !task.isCompletedAbnormally() && !task.isCompletedNormally())
                .forEach(
                        forkJoinTask -> {
                            try {
                                forkJoinTask.get(maxWaitTimeForTask.toMillis(), TimeUnit.MILLISECONDS);
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            } catch (TimeoutException ignore) {
                            }
                        }
                );

        return tasksInThreadPool.stream()
                .filter(task -> !task.isCompletedAbnormally() && !task.isCompletedNormally())
                .toList();
    }

    public static boolean isAllowedToAddTask(List<ForkJoinTask<?>> tasksInThreadPool, int maxConcurrency) {
        return tasksInThreadPool.size() < maxConcurrency;
    }

    public static List<ForkJoinTask<?>> filterOutFinishedTasks(List<ForkJoinTask<?>> tasksInThreadPool) {
        return tasksInThreadPool.stream()
                .filter(task -> !task.isCompletedAbnormally() && !task.isCompletedNormally())
                .toList();
    }

    public record Loops() {
        public record InputDto(
                ForkJoinPool forkJoinPool,
                List<TaskDto> tasks,
                Duration maxBackoff,
                int maxIsWorkIterations,
                int maxSuccessiveNoTasksCreated
        ) {
            public record TaskDto(
                    String name,
                    Supplier<Integer> maxConcurrency,
                    Supplier<Boolean> isWork,
                    Runnable runnable,
                    List<ForkJoinTask<?>> ongoingTasks
            ) {
            }
        }

        public record CalculationDto(
                InputDto.TaskDto task,
                AtomicReference<List<ForkJoinTask<?>>> tasksInThreadPool,
                AtomicInteger numIsWorkIterations,
                AtomicInteger tasksCreated
        ) {
        }

        public record ComputedDto(
                InputDto input,
                List<TaskDto> tasks,
                int totalNumTasksCreated,
                InputDto updatedInput
        ) {
            public record TaskDto(
                    InputDto.TaskDto inputTask,
                    CalculationDto calculation
            ) {
            }
        }

        public static ComputedDto runWhileWork(
                InputDto input
        ) {
            var tasks = input.tasks.stream()
                    .map(task ->
                            new ComputedDto.TaskDto(
                                    task,
                                    new CalculationDto(
                                            task,
                                            new AtomicReference<>(
                                                    filterOutFinishedTasks(task.ongoingTasks)
                                            ),
                                            new AtomicInteger(0),
                                            new AtomicInteger(0)
                                    )
                            )
                    )
                    .toList();

            int numIsWorkIterations = 0;
            int numSuccessiveNoTasksCreated = 0;
            int totalNumTasksCreated = 0;

            while (
                    numIsWorkIterations++ <= input.maxIsWorkIterations
                    &&
                    numSuccessiveNoTasksCreated <= input.maxSuccessiveNoTasksCreated
            ) {
                var tasksCreated =
                        tasks.stream()
                                .map(
                                        task ->
                                                runOnceIfWork(
                                                        input.forkJoinPool,
                                                        task.calculation,
                                                        task.calculation.task.isWork
                                                )
                                )
                                .mapToInt(calculation -> calculation.tasksCreated.get())
                                .sum();

                totalNumTasksCreated += tasksCreated;

                if (tasksCreated > 0) {
                    numSuccessiveNoTasksCreated = 0;
                } else {
                    ++numSuccessiveNoTasksCreated;

                    Loop.backoffWithSleep(
                            numSuccessiveNoTasksCreated,
                            input.maxBackoff
                    );
                }
            }

            return new ComputedDto(
                    input,
                    tasks,
                    totalNumTasksCreated,
                    new InputDto(
                            input.forkJoinPool,
                            tasks.stream()
                                    .map(task ->
                                            new InputDto.TaskDto(
                                                    task.inputTask.name,
                                                    task.inputTask.maxConcurrency,
                                                    task.inputTask.isWork,
                                                    task.inputTask.runnable(),
                                                    filterOutFinishedTasks(task.calculation.tasksInThreadPool.get())
                                            )
                                    )
                                    .toList(),
                            input.maxBackoff,
                            input.maxIsWorkIterations,
                            input.maxSuccessiveNoTasksCreated
                    )
            );
        }

        public static CalculationDto runOnceIfWork(
                ForkJoinPool forkJoinPool,
                CalculationDto calculation,
                Supplier<Boolean> isWork
        ) {
            var maxConcurrency = calculation.task.maxConcurrency.get();

            if (
                    isAllowedToAddTask(calculation.tasksInThreadPool.get(), maxConcurrency)
                    &&
                    isWork.get()
            ) {
                var computed =
                        Submit.submitRunnableTasks(
                                new Submit.InputDto(
                                        forkJoinPool,
                                        calculation.task.runnable,
                                        calculation.tasksInThreadPool.get(),
                                        maxConcurrency
                                )
                        );

                calculation.tasksInThreadPool.set(computed.tasksInThreadpool);
                calculation.tasksCreated.set(computed.tasksCreated);
            } else {
                calculation.tasksInThreadPool.updateAndGet(ComputeRunnableTask::filterOutFinishedTasks);
                calculation.tasksCreated.set(0);
            }
            return calculation;
        }
    }

    public record Loop() {
        public record InputDto(
                ForkJoinPool forkJoinPool,
                Supplier<Integer> maxConcurrency,
                Supplier<Boolean> isWork,
                Runnable runnable,
                List<ForkJoinTask<?>> ongoingTasks,
                Duration maxBackoff,
                int maxIsWorkIterations
        ) {
        }

        public record ComputedDto(
                List<ForkJoinTask<?>> tasksInThreadPool
        ) {
        }

        public static ComputedDto runWhileWork(
                InputDto input
        ) {
            int numIsWorkIterations = 0;
            int numSuccessiveNoTasksCreated = 0;

            var computed = new Submit.ComputedDto(input.ongoingTasks, 0);

            while (numIsWorkIterations++ < input.maxIsWorkIterations) {

                var allowedToAddTask = isAllowedToAddTask(computed.tasksInThreadpool, input.maxConcurrency.get());

                var isWork =
                        Optional.of(allowedToAddTask)
                                .filter(allowed -> allowed)
                                .map(allowed -> input.isWork.get());

                // if isWork optional is not empty and false then break out, otherwise try to add tasks
                if (!isWork.orElse(true)) {
                    // Break out reason: it is allowed to add tasks but there is no more work
                    break;
                }

                if (allowedToAddTask) {
                    computed =
                            Submit.submitRunnableTasks(
                                    new Submit.InputDto(
                                            input.forkJoinPool,
                                            input.runnable,
                                            computed.tasksInThreadpool,
                                            input.maxConcurrency.get()
                                    )
                            );
                } else {
                    computed =
                            new Submit.ComputedDto(
                                    filterOutFinishedTasks(computed.tasksInThreadpool),
                                    0
                            );
                }

                if (computed.tasksCreated > 0) {
                    numSuccessiveNoTasksCreated = 0;
                } else {
                    ++numSuccessiveNoTasksCreated;

                    Loop.backoffWithSleep(
                            numSuccessiveNoTasksCreated,
                            input.maxBackoff
                    );
                }
            }

            return new ComputedDto(
                    computed.tasksInThreadpool
            );
        }

        static void backoffWithSleep(long noTasksCreatedCounter, Duration maxBackoff) {
            try {
                TimeUnit.MILLISECONDS.sleep(
                        BackoffUtil.toExponentialBackoff(
                                        TimeUnit.MILLISECONDS,
                                        noTasksCreatedCounter,
                                        maxBackoff
                                )
                                .toMillis()
                );
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted task loop ", e);
            }
        }
    }

    public record Submit() {

        public record InputDto(
                ForkJoinPool forkJoinPool,
                Runnable runnable,
                List<ForkJoinTask<?>> ongoingTasks,
                int maxConcurrency
        ) {
        }

        public record ComputedDto(
                List<ForkJoinTask<?>> tasksInThreadpool,
                int tasksCreated
        ) {
        }

        /**
         * Receives forkJoinPool, runnable, ongoingTasks and a maxConcurrency.
         * <p>
         * Calculates number of tasksToCreate based on maxConcurrency and active tasks in ongoingTasks (still running).
         * <p>
         * Submit n=tasksToCreate to a java thread pool and returns the createdTasks
         * <p>
         * input.forkJoinPool   thread pool to execute tasks in.
         * input.runnable       the task to run
         * input.ongoingTasks   futures that were submitted to thread pool
         * input.maxConcurrency max number of non-completed futures allowed
         */
        public static ComputedDto submitRunnableTasks(
                InputDto input
        ) {
            return Submit.submitRunnableTasks(
                    input.ongoingTasks.stream()
                            .filter(task -> !task.isCompletedAbnormally() && !task.isCompletedNormally())
                            .toList(),
                    input.maxConcurrency,
                    tasksToCreate ->
                            Submit.submitNTasks(
                                    input.forkJoinPool,
                                    input.runnable,
                                    tasksToCreate
                            )
            );
        }

        public static ComputedDto submitRunnableTasks(
                List<ForkJoinTask<?>> activeTasks,
                Integer maxConcurrency,
                Function<Integer, List<ForkJoinTask<?>>> submitRunnable
        ) {
            return Optional.of(
                            maxConcurrency - activeTasks.size()
                    )
                    .map(tasksToCreate ->
                            new ComputedDto(
                                    Stream.concat(
                                                    activeTasks.stream(),
                                                    submitRunnable.apply(
                                                                    tasksToCreate
                                                            )
                                                            .stream()
                                            )
                                            .toList(),
                                    tasksToCreate
                            )
                    )
                    .orElseThrow();
        }

        public static List<ForkJoinTask<?>> submitNTasks(
                ForkJoinPool forkJoinPool,
                Runnable runnable,
                int tasksToCreate
        ) {
            if (tasksToCreate <= 0) {
                return Collections.emptyList();
            }

            return IntStream
                    .range(0, tasksToCreate)
                    .boxed()
                    .<ForkJoinTask<?>>map(value -> forkJoinPool.submit(runnable))
                    .toList();
        }
    }
}
