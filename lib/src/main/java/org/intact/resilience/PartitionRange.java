package org.intact.resilience;

import java.util.ArrayList;
import java.util.List;

public class PartitionRange {

    public interface Partitioner<V> {
        V newRange(long from, long to, V value);
    }

    // Inclusive [from,to] ranges
    public static <V> List<V> partition(
            V value,
            long inputFrom,
            long inputTo,
            long maxRangeSize,
            Partitioner<V> partitioner
    ) {
        if (inputFrom > inputTo) {
            throw new IllegalStateException("From cannot be larger than to: " + inputFrom + " > " + inputTo);
        }
        if (inputFrom == inputTo) {
            return List.of(value);
        }
        if ((inputTo - inputFrom) <= maxRangeSize) {
            return List.of(value);
        }


        long from = inputFrom;
        long to =
                Math.min(
                        inputTo,
                        inputFrom + maxRangeSize
                );

        var newRanges = new ArrayList<V>();

        while (from < to) {
            newRanges.add(
                    partitioner.newRange(from, to, value)
            );


            from = Math.min(
                    to + 1,
                    inputTo
            );

            to =
                    Math.min(
                            from + maxRangeSize,
                            inputTo
                    );
        }

        return newRanges;
    }
}
