package common;

import java.util.ArrayList;

/** An interface to read tuples from a file. */
public interface TupleReader {

  public Tuple readNextTuple();

  public ArrayList<Tuple> readAllTuples();

  public void close();

  public void reset();

  public void reset(int i);
}
