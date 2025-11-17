package org.intact.resilience;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class IdempotentBulk<T, K, V> {
    public enum Status {
        CREATED_AND_OK,
        CREATED,
        OK
    }

    private Function<Collection<T>, Map<K, V>> persistCommand;
    private Function<Collection<T>, Pair<Map<K, V>, Collection<T>>> onDuplicateKeyExceptionDo;

    public static <T, K, V> IdempotentBulk<T, K, V> withPersistCommand(Function<Collection<T>, Map<K, V>> persistTransactions) {
        var bulk = new IdempotentBulk<T, K, V>();
        bulk.persistCommand = requireNonNull(persistTransactions);
        return bulk;
    }

    public IdempotentBulk<T, K, V> onDuplicateKeyExceptionDo(Function<Collection<T>, Pair<Map<K, V>, Collection<T>>> onDuplicateKeyExceptionDo) {
        this.onDuplicateKeyExceptionDo = requireNonNull(onDuplicateKeyExceptionDo);
        return this;
    }

    public Data<K, V> persist(Collection<T> records) {
        requireNonNull(persistCommand);
        requireNonNull(onDuplicateKeyExceptionDo);

        var foundInDb = new HashMap<K, V>();
        var nonProgressingRecursionCounter = 0;

        var persistedInDb = Idempotent.persistToMap(
                records,
                persistCommand,
                onDuplicateKeyExceptionDo,
                foundInDb,
                nonProgressingRecursionCounter
        );

        return new Data<>(
                Map.of(
                        Status.CREATED_AND_OK, mergeAll(persistedInDb, foundInDb),
                        Status.OK, foundInDb,
                        Status.CREATED, persistedInDb
                )
        );
    }

    private static <K, V> Map<K, V> mergeAll(Map<K, V> persistedInDb, Map<K, V> foundInDb) {
        return foundInDb.isEmpty()
                ?
                persistedInDb
                :
                Stream.of(foundInDb, persistedInDb)
                        .flatMap(a -> a.entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static class Data<K, V> {
        public final Map<Status, Map<K, V>> values;

        public Data(Map<Status, Map<K, V>> values) {
            this.values = requireNonNull(values);
        }

        public Map<K, V> toCreatedAndOk() {
            return Optional.ofNullable(values.get(Status.CREATED_AND_OK)).orElse(emptyMap());
        }

        public Map<K, V> toOk() {
            return Optional.ofNullable(values.get(Status.OK)).orElse(emptyMap());
        }

        public Map<K, V> toCreated() {
            return Optional.ofNullable(values.get(Status.CREATED)).orElse(emptyMap());
        }

        public boolean isEmpty() {
            return values.entrySet().stream()
                    .allMatch(a -> a.getValue().isEmpty());
        }
    }
}
