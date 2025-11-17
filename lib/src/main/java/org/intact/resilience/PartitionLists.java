package org.intact.resilience;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public final class PartitionLists {
    private PartitionLists() {
    }

    public interface Partitioner<V> {
        V sublist(int from, int to, V value);
    }

    public static <V> List<V> splitInTwo(
            V value,
            int size,
            Partitioner<V> partitioner
    ) {
        if (size <= 1) {
            return List.of(value);
        }

        int mid = size / 2;

        return List.of(
                partitioner.sublist(0, mid, value),
                partitioner.sublist(mid, size, value)
        );
    }

    public static <V> List<V> toSubListIfExists(
            int from,
            int to,
            List<V> list
    ) {
        return from > list.size()
                ?
                Collections.emptyList()
                :
                to > list.size()
                        ?
                        list.subList(from, list.size())
                        :
                        list.subList(from, to);
    }


    /**
     * Partitions the list of values into List of Lists of values with a maxSize of the items.
     * <p>
     * Typical usage, each value V has a list of items. The size of this list should be returned by getSize.
     * The method will then partition the input so that the maxSize is never exceeded.
     * <p>
     * The maxSize is less than or equal to the sum of all values' getSize.apply(value).
     * <p>
     * This is a greedy algorithm of the bin packing problem with fragmentation, one iteration of the values, greedily fitting.
     * <a href="https://en.wikipedia.org/wiki/Bin_packing_problem#Bin-packing_with_fragmentation">Ref, bin packing problem with fragmentation</a>
     * <p>
     * See google implementation in guava for a reference implementation with list of lists.
     *
     * @param values  to be partitioned
     * @param getSize each value V has a size, typically the size of a list of items. The size of this list(s) should be returned by getSize.
     * @param maxSize maxSize is less than or equal to the sum of all values' getSize.apply(value).
     * @param <V>     value
     * @return List of lists of values with a maxSize in each list
     */
    public static <V> List<List<V>> partition(
            List<V> values,
            int maxSize,
            Function<V, Integer> getSize,
            Partitioner<V> partitioner
    ) {
        if (maxSize <= 0) {
            throw new IllegalStateException("Max size must be greater than zero, but was " + maxSize);
        }

        var listOfList = new ArrayList<List<V>>();
        var partition = new AtomicReference<List<V>>(new ArrayList<>());
        var currentSize = new AtomicInteger(0);

        for (var value : values) {

            if (currentSize.get() >= maxSize) {
                beginNewPartition(listOfList, partition, currentSize);
            }

            var sizeOfValue = getSize.apply(value);

            // No need to partition
            if (sizeOfValue <= 1) {
                currentSize.incrementAndGet();
                partition.get().add(value);
            }
            // Partitioning required
            else {
                partition(
                        value,
                        part -> {
                            var partitionSize = getSize.apply(part);

                            if (currentSize.get() + partitionSize > maxSize) {
                                beginNewPartition(listOfList, partition, currentSize);
                            }

                            currentSize.set(currentSize.get() + partitionSize);
                            partition.get().add(part);
                        },
                        partitioner,
                        maxSize,
                        sizeOfValue,
                        currentSize
                );
            }
        }

        beginNewPartition(listOfList, partition, currentSize);

        return listOfList;
    }

    private static <V> void beginNewPartition(
            ArrayList<List<V>> listOfList,
            AtomicReference<List<V>> partition,
            AtomicInteger currentSize
    ) {
        if (partition.get().isEmpty()) {
            return;
        }

        listOfList.add(partition.get());
        currentSize.set(0);
        partition.set(new ArrayList<>());
    }

    private static <V> void partition(
            V value,
            Consumer<V> addPartition,
            Partitioner<V> partitioner,
            int maxSize,
            int sizeOfValue,
            AtomicInteger currentSizeOfPartition
    ) {
        int from = 0;
        int to =
                Math.min(
                        Math.min(maxSize, sizeOfValue),
                        maxSize - currentSizeOfPartition.get()
                );

        while (from < to) {

            var part = partitioner.sublist(from, to, value);

            addPartition.accept(part);

            from = to;
            to =
                    Math.min(
                            from + maxSize,
                            from + (sizeOfValue - from)
                    );
        }
    }
}

