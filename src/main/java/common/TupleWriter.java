package common;

/** An interface to write tuples to a file. */
public interface TupleWriter {

  public void writeNextTuple(Tuple tuple);

  public void close();

  public void reset();
}
