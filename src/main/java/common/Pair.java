package common;

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
}
