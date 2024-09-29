package operator;

import common.BinaryHandler;
import common.DBCatalog;
import common.Tuple;
import common.TupleReader;

import java.util.ArrayList;
import net.sf.jsqlparser.schema.Table;

/**
 * An operator for SELECT *. It reads all rows of data from the file using
 * BufferReader
 */
public class ScanOperator extends Operator {
  // private BufferedReader bufferedReader;
  private TupleReader tupleReader;

  /**
   * ScanOperator constructor
   *
   * @param table table that needs to be scanned
   */
  public ScanOperator(Table table) {
    super(new ArrayList<>());
    try {
      this.tupleReader = new BinaryHandler(table.getName());
      this.outputSchema = DBCatalog.getInstance().getColumnsWithAlias(table);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /** re-initialize buffer reader */
  @Override
  public void reset() {
    try {
      this.tupleReader.reset();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * @return single row as tuple
   */
  @Override
  public Tuple getNextTuple() {
    Tuple tuple = this.tupleReader.readNextTuple();
    return tuple;
  }
}
