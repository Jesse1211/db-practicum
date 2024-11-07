package physical_operator;

import common.IndexDeserializer;
import common.Tuple;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;

/** file scan with retrieving a range of tuples from the table by using B+ tree index */
public class IndexScanOperator extends Operator {
  private IndexDeserializer indexDeserializer;

  /**
   * IndexScanOperator constructor
   *
   * @param lowKey low key of the range, can be null
   * @param highKey high key of the range, can be null
   */
  public IndexScanOperator(
      ArrayList<Column> outputSchema, int lowKey, int highKey, String tableName) {
    super(outputSchema);
    this.indexDeserializer = new IndexDeserializer(lowKey, highKey, tableName);
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'reset'");
  }

  @Override
  public Tuple getNextTuple() {
    return indexDeserializer.next();
  }
}
