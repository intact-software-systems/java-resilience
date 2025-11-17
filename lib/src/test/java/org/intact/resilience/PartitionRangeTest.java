package org.intact.resilience;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRangeTest {


    record Range(long from, long to) {
    }

    @Test
    void testPartitionRange() {
        var from = 100;
        var to = 999;
        var maxRangeSize = 100;

        var newRanges = PartitionRange.partition(
                new Range(from, to),
                from,
                to,
                maxRangeSize,
                (from1, to1, value) -> new Range(from1, to1)
        );


        assertEquals(Math.round((float) (to - from) / maxRangeSize), newRanges.size());

        newRanges
                .forEach(range -> {
                    assertTrue(range.from < range.to);
                    assertTrue(range.to <= to);
                    assertTrue(range.from >= from);
                    assertTrue(range.to - range.from <= maxRangeSize);
                });
    }
}