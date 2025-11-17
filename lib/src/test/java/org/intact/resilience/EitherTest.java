package org.intact.resilience;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EitherTest {
    record Dto(Integer number, String message) {
    }

    record ComputedDto(Integer number, String message) {
    }

    @Test
    void testEitherFolding() {
        var eithers = Stream.of(1, 2, 3, 4, 5)
                .map(number -> {
                    if (number > 3) {
                        return Map.entry(
                                number,
                                Either.<RuntimeException, Dto>ofLeft(new IllegalStateException("No numbers larger than 3: " + number))
                        );
                    }

                    return Map.entry(
                            number,
                            Either.<RuntimeException, Dto>ofRight(new Dto(number, "Number is good: " + number))
                    );
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


        {
            var runtimeExceptions = Either.EitherCollectors.toMapFoldLefts(eithers);
            var dtos = Either.EitherCollectors.toMapFoldRights(eithers);

            assertEquals(2, runtimeExceptions.size());
            assertEquals(3, dtos.size());
        }

        {
            var foldedBoth = Either.EitherCollectors.toMapFoldBoth(eithers);

            assertEquals(2, foldedBoth.leftByKey().size());
            assertEquals(3, foldedBoth.rightByKey().size());
        }

        {
            var computed = Either.Computers.toMapFoldComputer(
                    eithers,
                    (integer, e) -> Either.ofLeft(e),
                    (integer, dto) -> {
                        try {
                            if (dto.number == 1) {
                                throw new IllegalStateException("Nope, wrong number");
                            }

                            return Either.ofRight(new ComputedDto(dto.number, dto.message));
                        } catch (RuntimeException e) {
                            return Either.ofLeft(e);
                        }
                    }
            );

            var runtimeExceptions = Either.EitherCollectors.toMapFoldLefts(computed);
            var dtos = Either.EitherCollectors.toMapFoldRights(computed);

            assertEquals(5, computed.size());
            assertEquals(3, runtimeExceptions.size());
            assertEquals(2, dtos.size());
        }

        {
            var booleanByInteger = Either.Computers.toMapFoldToOneComputer(
                    eithers,
                    (integer, e) -> false,
                    (integer, dto) -> true
            );


            assertEquals(5, booleanByInteger.size());
            assertEquals(3, booleanByInteger.values().stream().filter(b -> b).count());
            assertEquals(2, booleanByInteger.values().stream().filter(b -> !b).count());
        }

    }

    @Test
    void testEitherList() {
        var eithers = Stream.of(1, 2, 3, 4, 5)
                .map(number -> {
                    if (number > 3) {
                        return Map.entry(
                                number,
                                List.of(
                                        Either.<RuntimeException, Dto>ofLeft(
                                                new IllegalStateException("No numbers larger than 3: " + number)
                                        )
                                )
                        );
                    }

                    return Map.entry(
                            number,
                            List.of(
                                    Either.<RuntimeException, Dto>ofRight(
                                            new Dto(number, "Number is good: " + number)
                                    )
                            )
                    );
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


        {
            var runtimeExceptions = Either.EitherCollectors.toMapFoldListOfLefts(eithers);
            var dtos = Either.EitherCollectors.toMapFoldListOfRights(eithers);

            assertEquals(2, runtimeExceptions.size());
            assertEquals(3, dtos.size());
        }

        {
            var foldedBoth = Either.EitherCollectors.toMapFoldListOfBoth(eithers);

            assertEquals(2, foldedBoth.leftsByKey().size());
            assertEquals(3, foldedBoth.rightsByKey().size());
        }
    }

    @Test
    void testEitherWithTry() {
        var eithers = Stream.of(1, 2, 3, 4, 5)
                .map(number ->
                        Either.Try.computeEntry(
                                number,
                                () -> {
                                    if (number > 3) {
                                        throw new IllegalStateException("No numbers larger than 3: " + number);
                                    }
                                    return new Dto(number, "Number is good: " + number);
                                }
                        )
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        var runtimeExceptions = Either.EitherCollectors.toMapFoldLefts(eithers);
        var dtos = Either.EitherCollectors.toMapFoldRights(eithers);

        assertEquals(2, runtimeExceptions.size());
        assertEquals(3, dtos.size());
    }

    @Test
    void testLazyComputation() {
        Map<Integer, Supplier<Dto>> suppliers = Stream.of(1, 2, 3, 4, 5)
                .collect(Collectors.toMap(
                        number -> number,
                        number ->
                                () -> {
                                    if (number > 3) {
                                        throw new IllegalStateException("No numbers larger than 3: " + (int) number);
                                    }
                                    return new Dto(number, "Number is good: " + (int) number);
                                }
                ));

        var eithers = Either.Try.computeAll(suppliers);

        var runtimeExceptions = Either.EitherCollectors.toMapFoldLefts(eithers);
        var dtos = Either.EitherCollectors.toMapFoldRights(eithers);

        assertEquals(2, runtimeExceptions.size());
        assertEquals(3, dtos.size());
    }

    @Test
    void testEitherTryCompute() {
        {
            var optionalDto = Either.Try
                    .<Dto>compute(() -> {
                        throw new IllegalStateException("Nope");
                    })
                    .mapBoth(
                            exception -> exception,
                            dto -> new Dto(dto.number + 1, "Yes again")
                    )
                    .foldRight(dto -> dto);

            assertTrue(optionalDto.isEmpty());
        }

        {
            var optionalDto = Either.Try
                    .compute(() -> new Dto(3, "Yes indeed"))
                    .mapBoth(
                            e -> e,
                            dto -> new Dto(dto.number + 1, "Yes again")
                    )
                    .foldRight(dto -> dto);

            assertTrue(optionalDto.isPresent());
            assertEquals(4, optionalDto.map(dto -> dto.number).orElse(0));
        }
    }
}
