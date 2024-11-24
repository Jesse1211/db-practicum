package common.pair;

import java.util.Objects;

/**
 * Initialize a new Pair object that have 2 objects.
 *
 * @param t object 1
 * @param u object 2
 * @param <T> type of object 1
 * @param <U> type of object 2
 */
public record Pair<T, U>(T t, U u) {

  /**
   * gets the left object from the pair
   *
   * @return the left object
   */
  public T getLeft() {
    return t;
  }

  /**
   * gets the right object from the pair
   *
   * @return the right object
   */
  public U getRight() {
    return u;
  }

  @Override
  public int hashCode() {
    // Treat (t, u) and (u, t) as the same by hashing both orders
    return Objects.hash(t) + Objects.hash(u);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Pair<?, ?> pair = (Pair<?, ?>) o;

    // equal if (t, u) matches (u, t)
    return (Objects.equals(t, pair.t) && Objects.equals(u, pair.u))
        || (Objects.equals(t, pair.u) && Objects.equals(u, pair.t));
  }

  @Override
  public String toString() {
    return "Pair{" + "left=" + t + ", right=" + u + '}';
  }
}
