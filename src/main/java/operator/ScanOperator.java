package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Table;

/** An operator for SELECT *. It reads all rows of data from the file using BufferReader */
public class ScanOperator extends Operator {
  private BufferedReader bufferedReader;
  private File file;

  /**
   * ScanOperator constructor
   *
   * @param table table that needs to be scanned
   */
  public ScanOperator(Table table) {
    super(new ArrayList<>());
    try {
      this.file = DBCatalog.getInstance().getFileForTable(table.getName());
      this.bufferedReader = new BufferedReader(new FileReader(file));
      this.outputSchema = DBCatalog.getInstance().getColumnsWithAlias(table);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /** re-initialize buffer reader */
  @Override
  public void reset() {
    try {
      bufferedReader.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }

    try {
      // Reopen to go back to the first line
      bufferedReader = new BufferedReader(new FileReader(file));
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * @return single row as tuple
   */
  @Override
  public Tuple getNextTuple() {
    try {
      String line;
      if ((line = bufferedReader.readLine()) != null) {
        return new Tuple(line);
      } else {
        bufferedReader.close();
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return null;
  }
}
