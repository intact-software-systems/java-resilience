package org.intact.resilience;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.LongStream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotentTest {

    @Test
    void persistOne() {
        String key = "1";
        Set<String> persisted = Idempotent.persist(Map.of(key, key), Map::keySet);

        assertTrue(persisted.contains(key));
    }

    @Test
    void branchOutMaxAndPersistAll() {
        Map<Long, Long> records = LongStream
                .iterate(0, i -> i < 500, i -> i + 1)
                .mapToObj(cnt -> Map.entry(cnt, cnt))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));


        var persisted = Idempotent.persist(
                records,
                stringStringMap -> {
                    if (stringStringMap.size() != 1) {
                        throw new DuplicateKeyException("duplicate");
                    }
                    return stringStringMap.keySet();
                });

        assertEquals(records.size(), persisted.size());
    }

    @Test
    void persistNone() {
        Map<Long, Long> records = LongStream
                .iterate(0, i -> i < 500, i -> i + 1)
                .mapToObj(cnt -> Map.entry(cnt, cnt))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));


        var persisted = Idempotent.persist(
                records,
                stringStringMap -> {
                    throw new DuplicateKeyException("duplicate");
                });

        assertEquals(records.size(), persisted.size());
    }

    @Test
    void persistRandom() {
        Map<Long, Long> records = LongStream
                .iterate(0, i -> i < 500, i -> i + 1)
                .mapToObj(cnt -> Map.entry(cnt, cnt))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));


        var persisted = Idempotent.persist(
                records,
                stringStringMap -> {
                    if (Math.random() > 0.5) {
                        throw new DuplicateKeyException("duplicate");
                    }
                    return stringStringMap.keySet();
                });

        assertEquals(records.size(), persisted.size());
    }

    @Test
    public void testIdempotentAlgorithms() {
        var resourceId = UUID.randomUUID().toString();
        var records = Map.of(resourceId, "Record with values");
        var foundInDb = Map.of(resourceId, Long.valueOf(12L));
        var alreadyPersisted = new HashMap<String, Long>();

        var nonProgressingRecursionCounter = 0;
        Map<String, Long> returned = Idempotent.persistToMap(
                records.values(),
                vos -> {
                    throw new DuplicateKeyException("duplicates");
                },
                vos -> Pair.of(foundInDb, emptyList()),
                alreadyPersisted,
                nonProgressingRecursionCounter
        );

        assertTrue(returned.isEmpty());
        assertEquals(alreadyPersisted.size(), records.size());
        assertFalse(alreadyPersisted.isEmpty());
        assertEquals(alreadyPersisted.size(), foundInDb.size());
        assertEquals(0, nonProgressingRecursionCounter);
    }
}
