package org.intact.resilience;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionListsTest {
    private static final Logger log = LoggerFactory.getLogger(PartitionListsTest.class);

    private static final int START_INCLUSIVE = 0;
    private static final Random RANDOM = new Random();

    record Data(
            List<Integer> data
    ) {
    }

    @RepeatedTest(1000)
    void testPartition() {
        testPartitioning(
                Math.max(1, RANDOM.nextInt(100)),
                Math.max(1, RANDOM.nextInt(100))
        );
    }

    @Test
    void testZeroSizeOfCollection() {
        var endExclusive = 50;
        var maxSize = 10;

        var data = IntStream.range(START_INCLUSIVE, endExclusive)
                .boxed()
                .map(integer -> new Data(Collections.emptyList()))
                .toList();


        var partitions = PartitionLists.partition(
                data,
                maxSize,
                data1 -> data1.data.size(),
                (from, to, d) -> new Data(d.data.subList(from, to))
        );

        var expectedNumOfPartitions = 5;
        assertEquals(expectedNumOfPartitions, partitions.size());

        partitions.forEach(
                d -> assertEquals(maxSize, d.size())
        );
    }

    @Test
    void testWhenLargerMaxSizeThanPartitions() {
        testPartitioning(10, 100);
    }

    @Test
    void testLargeInputSmallMaxSize() {
        testPartitioning(2000, 50);
    }

    @RepeatedTest(1000)
    void testOneInputWithLargeSubLists() {
        testOneInputWithLargeSubLists(
                Math.max(1, RANDOM.nextInt(100)),
                Math.max(1, RANDOM.nextInt(100)),
                Math.max(1, RANDOM.nextInt(100))
        );
    }

    @Test
    void testOneInputWithEmptySubLists() {
        testOneInputWithLargeSubLists(
                Math.max(1, RANDOM.nextInt(100)),
                0,
                0
        );
    }

    void testOneInputWithLargeSubLists(int maxSize, int integerSize, int stringSize) {
        record DataWithTwoLists(
                List<Integer> integers,
                List<String> strings
        ) {
        }

        var dataWithTwoLists = new DataWithTwoLists(
                IntStream.range(START_INCLUSIVE, integerSize)
                        .boxed()
                        .toList(),
                IntStream.range(START_INCLUSIVE, stringSize)
                        .boxed()
                        .map(Object::toString)
                        .toList()
        );


        var partitions = PartitionLists.partition(
                List.of(dataWithTwoLists),
                maxSize,
                data1 ->
                        Math.max(dataWithTwoLists.integers.size(), dataWithTwoLists.strings.size()),
                (from, to, d) ->
                        new DataWithTwoLists(
                                PartitionLists.toSubListIfExists(from, to, d.integers),
                                PartitionLists.toSubListIfExists(from, to, d.strings)
                        )
        );


        var expectedNumOfPartitions = Math.max(1, Math.max(integerSize, (double) stringSize) / (double) maxSize);

        if (expectedNumOfPartitions != (int) expectedNumOfPartitions) {
            expectedNumOfPartitions = Math.floorDiv(Math.max(integerSize, stringSize), maxSize) + 1;
        }

        if (expectedNumOfPartitions != partitions.size()) {
            log.error("{} {} {} expected partition size {} was {}", maxSize, integerSize, stringSize, expectedNumOfPartitions, partitions.size());
        }

        assertEquals(
                expectedNumOfPartitions,
                partitions.size()
        );

        assertEquals(
                expectedNumOfPartitions,
                partitions
                        .stream()
                        .flatMap(Collection::stream)
                        .toList()
                        .size()
        );

        var allIntegers = new ArrayList<Integer>();
        var allStrings = new ArrayList<String>();

        partitions
                .stream()
                .flatMap(Collection::stream)
                .forEach(d -> {
                            allIntegers.addAll(d.integers);
                            allStrings.addAll(d.strings);

                            assertTrue(d.integers.size() <= maxSize);
                            assertTrue(d.strings.size() <= maxSize);
                        }
                );

        assertEquals(integerSize, allIntegers.size());
        assertEquals(stringSize, allStrings.size());
    }

    @Test
    void splitInTwoTest() {
        var data = new Data(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11));

        var split = PartitionLists.splitInTwo(
                data,
                data.data().size(),
                (from, to, value) -> new Data(value.data.subList(from, to))
        );

        assertEquals(2, split.size());
        assertEquals(11, split.stream().mapToInt(d -> d.data.size()).sum());
        assertEquals(split.get(0).data, List.of(1, 2, 3, 4, 5));
        assertEquals(split.get(1).data, List.of(6, 7, 8, 9, 10, 11));
    }

    private static void testPartitioning(int endExclusive, int maxSize) {
        var data = IntStream.range(START_INCLUSIVE, endExclusive)
                .boxed()
                .map(integer ->
                        new Data(
                                IntStream.range(START_INCLUSIVE, endExclusive)
                                        .boxed()
                                        .toList()
                        )
                )
                .toList();

        var partitions = PartitionLists.partition(
                data,
                maxSize,
                data1 -> data1.data.size(),
                (from, to, d) -> new Data(d.data.subList(from, to))
        );

        partitions.stream()
                .map(partition ->
                        partition.stream()
                                .map(d -> d.data.size())
                                .reduce(Integer::sum)
                                .orElse(0)
                )
                .forEach(numOfIntegersInPartition ->
                        assertTrue(numOfIntegersInPartition <= maxSize)
                );


        var expectedNumOfIntegers = endExclusive * endExclusive;
        var actualNumOfIntegers = toPartitionsNumOfIntegers(partitions);

        assertEquals(expectedNumOfIntegers, actualNumOfIntegers);


        var expectedSumOfIntegers = toSumOfIntegers(data);
        var actualSumOfIntegers = toPartitionsSumOfIntegers(partitions);

        assertEquals(expectedSumOfIntegers, actualSumOfIntegers);

        assertTrue(partitions.size() <= expectedNumOfIntegers);
    }

    private static int toPartitionsNumOfIntegers(List<List<Data>> partitions) {
        return partitions.stream()
                .mapToInt(d -> d.stream().mapToInt(d1 -> d1.data.size()).sum())
                .sum();
    }

    private static int toPartitionsSumOfIntegers(List<List<Data>> partitions) {
        return partitions.stream()
                .mapToInt(PartitionListsTest::toSumOfIntegers)
                .sum();
    }

    private static int toSumOfIntegers(List<Data> data) {
        return data.stream()
                .flatMap(d -> d.data.stream())
                .reduce(Integer::sum)
                .orElse(0);
    }
}