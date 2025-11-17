package org.intact.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public final class Idempotent {
    private static final Logger log = LoggerFactory.getLogger(Idempotent.class);
    private static final int NON_PROGRESSING_RECURSION_MAX_LIMIT = 10;

    public static <K, V> Set<K> persist(Map<K, V> records, Function<Map<K, V>, Set<K>> persistRecords) {
        try {
            return ofNullable(persistRecords.apply(records)).orElseGet(HashSet::new);
        } catch (DuplicateKeyException e) {
            if (records.size() > 1) {
                var split = split(records);

                var leftResult = persist(split.left(), persistRecords);
                var rightResult = persist(split.right(), persistRecords);

                var all = new HashSet<>(leftResult);
                all.addAll(rightResult);
                return all;
            } else {
                return new HashSet<>(records.keySet()); // Record already stored in DB
            }
        }
    }

    public static <K, V> Set<K> persist(Collection<V> records,
                                        Function<Collection<V>, Set<K>> persistRecords,
                                        Function<Collection<V>, Pair<Set<K>, Collection<V>>> onDuplicateReadDb) {
        try {
            return ofNullable(persistRecords.apply(records)).orElseGet(HashSet::new);
        } catch (DuplicateKeyException e) {
            log.trace("DuplicateKeyException: Number of records {}. Remove duplicates and retry.", records.size());

            var result = onDuplicateReadDb.apply(records);

            return result.right().isEmpty()
                    ? result.left()
                    : persist(result.right(), persistRecords, onDuplicateReadDb);
        }
    }

    public static <T, K, V> Map<K, V> persistToMap(Collection<T> records,
                                                   Function<Collection<T>, Map<K, V>> persistTransactions,
                                                   Function<Collection<T>, Pair<Map<K, V>, Collection<T>>> onDuplicateKeyExceptionDo,
                                                   Map<K, V> alreadyStored,
                                                   long nonProgressingRecursionCounter) {
        try {
            return ofNullable(persistTransactions.apply(records)).orElseGet(HashMap::new);
        } catch (DuplicateKeyException e) {
            var result = onDuplicateKeyExceptionDo.apply(records);
            alreadyStored.putAll(result.left());

            if(result.left().isEmpty()) {
                ++nonProgressingRecursionCounter;
            }

            if(nonProgressingRecursionCounter > NON_PROGRESSING_RECURSION_MAX_LIMIT) {
                throw new IllegalStateException("Non progressing recursion detected > " + NON_PROGRESSING_RECURSION_MAX_LIMIT + " . Give up. Issues in uniqueness in records.", e);
            }

            return result.right().isEmpty()
                    ? new HashMap<>()
                    : persistToMap(result.right(), persistTransactions, onDuplicateKeyExceptionDo, alreadyStored, nonProgressingRecursionCounter);
        }
    }

    public static <K, V> Pair<Map<K, V>, Map<K, V>> split(Map<K, V> values) {
        int midpoint = Math.max(1, values.size() / 2);
        AtomicInteger count = new AtomicInteger();

        Map<K, V> left = new HashMap<>();
        Map<K, V> right = new HashMap<>();

        values.forEach((key, value) -> {
            int index = count.getAndIncrement();

            if (index < midpoint) {
                left.put(key, value);
            } else {
                right.put(key, value);
            }
        });

        return new Pair<>(left, right);
    }

    private Idempotent() {
    }
}
