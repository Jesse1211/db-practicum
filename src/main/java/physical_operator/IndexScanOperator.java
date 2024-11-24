package physical_operator;

import compiler.DBCatalog;
import common.HelperMethods;
import common.index.IndexDeserializer;
import common.index.IndexInfo;
import common.tuple.Tuple;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/** file scan with retrieving a range of tuples from the table by using B+ tree index */
public class IndexScanOperator extends Operator {
  private IndexDeserializer indexDeserializer;

  /**
   * IndexScanOperator constructor
   *
   * @param lowKey low key of the range, can be null
   * @param highKey high key of the range, can be null
   */
  public IndexScanOperator(ArrayList<Column> outputSchema, int lowKey, int highKey, Table table) {
    super(outputSchema);
    IndexInfo indexInfo = DBCatalog.getInstance().getIndexInfo(table.getName());
    int attributeIndex =
        HelperMethods.mapColumnIndex(outputSchema, false)
            .get(indexInfo.relationName + "." + indexInfo.attributeName);
    this.indexDeserializer = new IndexDeserializer(lowKey, highKey, indexInfo, attributeIndex);
  }

  @Override
  public void reset() {
    this.indexDeserializer.reset();
  }

  @Override
  public Tuple getNextTuple() {
    return indexDeserializer.next();
  }
}
