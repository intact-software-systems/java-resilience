package org.intact.resilience;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * An either consisting of two elements; 'left' and 'right'.
 * Either left is non-null or right is non-null.
 * Right is typically the success
 * Left is typically the failure
 * <p>
 * Both can not be null.
 * Both can not be non-null.
 * <p>
 *
 * @param <L> left element
 * @param <R> right element
 */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "unused"})
public class Either<L, R> {
    public final Optional<L> left;
    public final Optional<R> right;

    // NB! Don't use, needed by Jackson json parser
    public Either() {
        this.left = Optional.empty();
        this.right = Optional.empty();
    }

    private Either(Optional<L> left, Optional<R> right) {
        this.left = requireNonNull(left);
        this.right = requireNonNull(right);
    }

    public static <L, R> Either<L, R> either(Supplier<L> leftSupplier, Supplier<R> rightSupplier) {
        return Optional.ofNullable(rightSupplier.get())
                .map(Either::<L, R>ofRight)
                .orElseGet(() -> Either.ofLeft(leftSupplier.get()));
    }

    public static <L, R> Either<L, R> ofLeft(L left) {
        return new Either<>(Optional.of(left), Optional.empty());
    }

    public static <L, R> Either<L, R> ofRight(R right) {
        return new Either<>(Optional.empty(), Optional.of(right));
    }

    public <T> T fold(Function<L, T> mapLeft, Function<R, T> mapRight) {
        return right
                .map(mapRight)
                .orElseGet(() -> mapLeft.apply(left.orElseThrow(() -> new IllegalStateException("No value present in left"))));
    }

    public <T> Optional<T> foldRight(Function<R, T> mapRight) {
        return right.map(mapRight);
    }

    public <T> Optional<T> foldLeft(Function<L, T> mapLeft) {
        return left.map(mapLeft);
    }

    public <T, U> Either<T, U> mapBoth(Function<L, T> mapLeft, Function<R, U> mapRight) {
        return right
                .map(value -> Either.<T, U>ofRight(mapRight.apply(value)))
                .orElseGet(() -> Either.ofLeft(mapLeft.apply(left.orElseThrow(() -> new IllegalStateException("No value present in left")))));
    }

    public <R2> Either<L, R2> mapRight(Function<R, R2> mapRight) {
        return mapBoth(Function.identity(), mapRight);
    }

    public <L2> Either<L2, R> mapLeft(Function<L, L2> mapLeft) {
        return mapBoth(mapLeft, Function.identity());
    }

    public <T, U> Either<T, U> flatMap(Function<L, Either<T, U>> mapLeft, Function<R, Either<T, U>> mapRight) {
        return right
                .map(mapRight)
                .orElseGet(() -> mapLeft.apply(left.orElseThrow(() -> new IllegalStateException("No value present in left"))));

    }

    public record EitherCollectors() {
        public static <K, L, R> Map<K, L> toMapFoldLefts(Map<K, Either<L, R>> eithers) {
            return eithers.entrySet().stream()
                    .flatMap(entry ->
                            entry.getValue()
                                    .foldLeft(e -> e)
                                    .stream()
                                    .map(e -> Map.entry(entry.getKey(), e))
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public static <K, L, R> Map<K, R> toMapFoldRights(Map<K, Either<L, R>> eithers) {
            return eithers.entrySet().stream()
                    .flatMap(entry ->
                            entry.getValue()
                                    .foldRight(e -> e)
                                    .stream()
                                    .map(e -> Map.entry(entry.getKey(), e))
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public record FoldComputedDto<K, L, R>(
                Map<K, L> leftByKey,
                Map<K, R> rightByKey
        ) {
        }

        public static <K, L, R> FoldComputedDto<K, L, R> toMapFoldBoth(Map<K, Either<L, R>> eitherByKey) {
            var leftByKey = new HashMap<K, L>();
            var rightByKey = new HashMap<K, R>();

            eitherByKey.forEach(
                    (k, either) -> {
                        either.left.ifPresent(l -> leftByKey.put(k, l));
                        either.right.ifPresent(r -> rightByKey.put(k, r));
                    }
            );

            return new FoldComputedDto<>(
                    leftByKey,
                    rightByKey
            );
        }

        public static <K, L, R> Map<K, List<L>> toMapFoldListOfLefts(Map<K, List<Either<L, R>>> eithers) {
            return eithers.entrySet().stream()
                    .map(entry ->
                            Map.entry(
                                    entry.getKey(),
                                    entry.getValue().stream()
                                            .flatMap(e -> e.foldLeft(l -> l).stream())
                                            .toList()
                            )
                    )
                    .filter(entry -> !entry.getValue().isEmpty())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public static <K, L, R> Map<K, List<R>> toMapFoldListOfRights(Map<K, List<Either<L, R>>> eithers) {
            return eithers.entrySet().stream()
                    .map(entry ->
                            Map.entry(
                                    entry.getKey(),
                                    entry.getValue().stream()
                                            .flatMap(e -> e.foldRight(l -> l).stream())
                                            .toList()
                            )
                    )
                    .filter(entry -> !entry.getValue().isEmpty())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public record FoldListComputedDto<K, L, R>(
                Map<K, List<L>> leftsByKey,
                Map<K, List<R>> rightsByKey
        ) {
        }

        public static <K, L, R> FoldListComputedDto<K, L, R> toMapFoldListOfBoth(Map<K, List<Either<L, R>>> eithersByKey) {
            var leftsByKey = new HashMap<K, List<L>>();
            var rightsByKey = new HashMap<K, List<R>>();

            eithersByKey
                    .forEach((k, eithers) ->
                            {
                                var lefts = new ArrayList<L>();
                                var rights = new ArrayList<R>();

                                eithers
                                        .forEach(e ->
                                                e.fold(
                                                        lefts::add,
                                                        rights::add
                                                )
                                        );

                                if (!lefts.isEmpty()) {
                                    leftsByKey.put(k, lefts);
                                }

                                if (!rights.isEmpty()) {
                                    rightsByKey.put(k, rights);
                                }
                            }
                    );

            return new FoldListComputedDto<>(
                    leftsByKey,
                    rightsByKey
            );
        }

        public static <L, R> List<L> toListFoldLefts(Collection<Either<L, R>> eithers) {
            return eithers.stream()
                    .flatMap(either -> either.foldLeft(e -> e).stream())
                    .toList();
        }

        public static <L, R> List<R> toListFoldRights(Collection<Either<L, R>> eithers) {
            return eithers.stream()
                    .flatMap(either -> either.foldRight(e -> e).stream())
                    .toList();
        }
    }

    public record Computers() {
        public static <K, V, L, R> Map<K, V> toMapFoldToOneComputer(
                Map<K, Either<L, R>> eithers,
                BiFunction<K, L, V> foldLeft,
                BiFunction<K, R, V> foldRightFunction
        ) {
            return eithers.entrySet().stream()
                    .map(entry ->
                            Map.entry(
                                    entry.getKey(),
                                    entry.getValue()
                                            .fold(
                                                    e -> foldLeft.apply(entry.getKey(), e),
                                                    val -> foldRightFunction.apply(entry.getKey(), val)
                                            )
                            )
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public static <K, T, U, L, R> Map<K, Either<T, U>> toMapFoldComputer(
                Map<K, Either<L, R>> eithers,
                BiFunction<K, L, Either<T, U>> foldLeft,
                BiFunction<K, R, Either<T, U>> foldRightFunction
        ) {
            return eithers.entrySet().stream()
                    .map(entry ->
                            Map.entry(
                                    entry.getKey(),
                                    entry.getValue()
                                            .fold(
                                                    e -> foldLeft.apply(entry.getKey(), e),
                                                    val -> foldRightFunction.apply(entry.getKey(), val)
                                            )
                            )
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public static <K, T, U, L, R> Map<K, Either<T, U>> toMapBothComputer(
                Map<K, Either<L, R>> eithers,
                BiFunction<K, L, T> foldLeft,
                BiFunction<K, R, U> foldRightFunction
        ) {
            return eithers.entrySet().stream()
                    .map(entry ->
                            Map.entry(
                                    entry.getKey(),
                                    entry.getValue()
                                            .mapBoth(
                                                    e -> foldLeft.apply(entry.getKey(), e),
                                                    val -> foldRightFunction.apply(entry.getKey(), val)
                                            )
                            )
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public static <K, T, U, L, R> Map<K, List<Either<T, U>>> toMapListBothComputer(
                Map<K, List<Either<L, R>>> eithers,
                BiFunction<K, L, T> foldLeft,
                BiFunction<K, R, U> foldRightFunction
        ) {
            return eithers.entrySet().stream()
                    .map(entry ->
                            Map.entry(
                                    entry.getKey(),
                                    entry.getValue().stream()
                                            .map(
                                                    lrEither -> lrEither
                                                            .mapBoth(
                                                                    e -> foldLeft.apply(entry.getKey(), e),
                                                                    val -> foldRightFunction.apply(entry.getKey(), val)
                                                            )
                                            )
                                            .toList()
                            )
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    public record Try() {
        public static <R> Either<RuntimeException, R> compute(Supplier<R> supplier) {
            try {
                return Either.ofRight(supplier.get());
            } catch (RuntimeException ex) {
                return Either.ofLeft(ex);
            }
        }

        public static <K, R> Map.Entry<K, Either<RuntimeException, R>> computeEntry(K key, Supplier<R> supplier) {
            try {
                return Map.entry(key, Either.ofRight(supplier.get()));
            } catch (RuntimeException ex) {
                return Map.entry(key, Either.ofLeft(ex));
            }
        }

        public static <K, R> Map<K, Either<RuntimeException, R>> computeAll(Map<K, Supplier<R>> lazyMap) {
            return lazyMap.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> Try.compute(entry.getValue()))
                    );
        }
    }
}
