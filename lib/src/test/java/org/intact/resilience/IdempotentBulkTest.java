package org.intact.resilience;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotentBulkTest {

    @Test
    public void givenNonExistingRecords_CreateAll() {
        var records = List.of("1", "2", "3", "4", "5");

        var data = IdempotentBulk
                .withPersistCommand((Collection<String> strings) -> strings.stream().collect(Collectors.toMap(a -> a, a -> a)))
                .onDuplicateKeyExceptionDo((Collection<String> strings) -> Pair.of(
                        Collections.emptyMap(),
                        Collections.emptyList()
                ))
                .persist(records);

        assertTrue(data.toOk().isEmpty());
        assertEquals(records.size(), data.toCreated().size());
    }

    @Test
    public void givenSomeExistingAndNewRecords_VerifyResultSplitInOkAndCreated() {
        var recordsToBeCreated = List.of("1", "2", "5");
        var recordsCausingDuplicateKeyException = List.of("3", "4");
        var allRecords = Stream.of(recordsToBeCreated, recordsCausingDuplicateKeyException).flatMap(Collection::stream).collect(Collectors.toList());

        var data = IdempotentBulk
                .withPersistCommand((Collection<String> strings) -> {
                    if (strings.stream().anyMatch(recordsCausingDuplicateKeyException::contains)) {
                        throw new DuplicateKeyException("Duplicate found");
                    }
                    return strings.stream().collect(Collectors.toMap(a -> a, a -> a));
                })
                .onDuplicateKeyExceptionDo((Collection<String> strings) -> {
                    var canAdd = strings.stream().filter(recordsToBeCreated::contains).collect(Collectors.toList());
                    var existing = strings.stream().filter(recordsCausingDuplicateKeyException::contains).collect(Collectors.toMap(k -> k, v -> v));
                    return Pair.of(
                            existing,
                            canAdd
                    );
                })
                .persist(allRecords);

        assertFalse(data.toOk().isEmpty());
        assertFalse(data.toCreated().isEmpty());
        assertEquals(recordsToBeCreated.size(), data.toCreated().size());
        assertEquals(recordsCausingDuplicateKeyException.size(), data.toOk().size());
        assertTrue(recordsCausingDuplicateKeyException.containsAll(data.toOk().values()));
        assertTrue(recordsToBeCreated.containsAll(data.toCreated().values()));
        assertTrue(allRecords.containsAll(data.toCreatedAndOk().values()));
    }
}
