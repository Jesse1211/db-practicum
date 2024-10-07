package common;

import java.util.ArrayList;

public interface TupleReader {

    public Tuple readNextTuple();

    public ArrayList<Tuple> readAllTuples();

    public void close();

    public void reset();
}
