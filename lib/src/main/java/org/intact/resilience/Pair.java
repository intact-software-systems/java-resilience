package org.intact.resilience;

import static java.util.Objects.requireNonNull;

/**
 * A pair consisting of two elements.
 * It refers to the elements as 'left' and 'right'.
 *
 * @param <L> left element
 * @param <R> right element
 */
public record Pair<L, R>(L left, R right) {

    public Pair(L left, R right) {
        this.left = requireNonNull(left);
        this.right = requireNonNull(right);
    }

    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }
}
