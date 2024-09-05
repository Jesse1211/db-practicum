package common;

public class Pair<T, U> {

  public final T t;
  public final U u;

  public Pair(T t, U u) {
    this.t = t;
    this.u = u;
  }

  public T getLeft() {
    return t;
  }

  public U getRight() {
    return u;
  }
}
