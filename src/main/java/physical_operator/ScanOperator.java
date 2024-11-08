package physical_operator;

import common.BinaryHandler;
import common.DBCatalog;
import common.Tuple;
import common.TupleReader;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Table;

/** An operator for SELECT *. It reads all rows of data from the file using BufferReader */
public class ScanOperator extends Operator {
  // private BufferedReader bufferedReader;
  private TupleReader tupleReader;
  private IndexDeserializer indexDeserializer;
  private boolean useIndex;

  /**
   * ScanOperator constructor
   *
   * @param table table that needs to be scanned
   */
  public ScanOperator(Table table) {
    super(new ArrayList<>());
      this.useIndex = DBCatalog.getInstance().getUseIndex();
      if (useIndex) {
        indexDeserializer = new IndexDeserializer(Integer.MAX_VALUE, Integer.MAX_VALUE, table.getName());
      } else {
        this.tupleReader = new BinaryHandler(table.getName());
      }

      // this.tupleReader = new TextHandler(table.getName());
      this.outputSchema = DBCatalog.getInstance().getColumnsWithAlias(table);
    
  }

  /** re-initialize buffer reader */
  @Override
  public void reset() {
    this.tupleReader.reset();
  }

  /**
   * @return single row as tuple
   */
  @Override
  public Tuple getNextTuple() {
    if (useIndex) {
      return indexDeserializer.next();
    }
    return this.tupleReader.readNextTuple();
  }
}
