package common;

public interface TupleReader {

    public Tuple readNextTuple();

    public void close();

    public void reset();

    public int getAttributeNum();

    public int getTuplesNum();
}
