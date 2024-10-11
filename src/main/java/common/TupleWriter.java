package common;

public interface TupleWriter {

  public void writeNextTuple(Tuple tuple);

  public void close();

  public void reset();
}
