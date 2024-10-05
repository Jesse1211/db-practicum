package common;

public interface TupleWriter {

  public void writeTuple(Tuple tuple);

  public void close();

  public void reset();
}
