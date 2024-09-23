package common;

public interface TupleWriter {

    public void writeTuple(Tuple tuple);

    public void close();

    public void reset();

    public int getAttributeNum();

    public int getTuplesNum();
}
