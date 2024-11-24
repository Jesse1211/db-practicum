package common.tuple;

import common.pair.Pair;
import java.util.ArrayList;

/** An interface to read tuples from a file. */
public interface TupleReader {

  public Tuple readNextTuple();

  public Pair<Tuple, Pair<Integer, Integer>> readNextTupleAndRid();

  public ArrayList<Tuple> readAllTuples();

  public void close();

  public void reset();

  public void reset(int i);

  public void reset(int pageIndex, int tupleIndex);
}
