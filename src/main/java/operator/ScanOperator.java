package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Table;

public class ScanOperator extends Operator {
  private BufferedReader bufferedReader;
  private File file;

  /**
   * Scan all info from the Table by Buffer Reader
   * 
   * @param table single table
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

  /**
   * re-initialize buffer reader
   */
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
   * Return single row as tuple
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
