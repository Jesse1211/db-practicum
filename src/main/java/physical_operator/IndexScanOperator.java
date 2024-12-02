package physical_operator;

import common.HelperMethods;
import common.index.IndexDeserializer;
import common.index.IndexInfo;
import common.pair.Pair;
import common.tuple.Tuple;
import compiler.DBCatalog;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/** file scan with retrieving a range of tuples from the table by using B+ tree index */
public class IndexScanOperator extends Operator {
  private IndexDeserializer indexDeserializer;

  /**
   * * IndexScanOperator constructor
   * @param lowKey low key of the range
   * @param highKey high key of the range
   * @param table relation
   * @param attributeName name of the indexed column
   */
  public IndexScanOperator(int lowKey, int highKey, Table table, String attributeName) {
    super(new ArrayList<>());
    this.outputSchema = DBCatalog.getInstance().getColumnsWithAlias(table);
    IndexInfo indexInfo = DBCatalog.getInstance().getIndexInfo(table.getName());
    int attributeIndex =HelperMethods.mapColumnIndex(outputSchema, false).get(
            indexInfo.relationName + "." + attributeName
    );
    this.indexDeserializer = new IndexDeserializer(
            lowKey,
            highKey,
            indexInfo.relationName,
            attributeName,
            indexInfo.attributes.get(attributeName).getLeft(),
            attributeIndex
    );
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
