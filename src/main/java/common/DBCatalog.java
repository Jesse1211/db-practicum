package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to contain information about database - names of tables, schema of each table and file
 * where each table is located. Uses singleton pattern.
 *
 * <p>Assumes dbDirectory has a schema.txt file and a /data subdirectory containing one file per
 * relation, named "relname".
 *
 * <p>Call by using DBCatalog.getInstance();
 */
public class DBCatalog {
  private final Logger logger = LogManager.getLogger();

  private final HashMap<String, ArrayList<Column>> tables;
  private static DBCatalog db;

  private String dbDirectory;

  // For Plan Builder Config
  private String joinMethod;
  private int joinBufferPageNumber = -1; // -1 means not set
  private String sortMethod;
  private int sortBufferPageNumber = -1; // -1 means not set

  /** Reads schemaFile and populates schema information */
  private DBCatalog() {
    tables = new HashMap<>();
  }

  /**
   * Instance getter for singleton pattern, lazy initialization on first invocation
   *
   * @return unique DB catalog instance
   */
  public static DBCatalog getInstance() {
    if (db == null) {
      db = new DBCatalog();
    }
    return db;
  }

  /**
   * Sets the data directory for the database catalog.
   *
   * @param directory: The input directory.
   */
  public void setDataDirectory(String directory) {
    try {
      dbDirectory = directory;
      BufferedReader br = new BufferedReader(new FileReader(directory + "/schema.txt"));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s");
        String tableName = tokens[0];
        // https://www.javadoc.io/doc/com.github.jsqlparser/jsqlparser/latest/net.sf.jsqlparser/net/sf/jsqlparser/schema/Column.html
        ArrayList<Column> cols = new ArrayList<>();
        for (int i = 1; i < tokens.length; i++) {
          cols.add(new Column(new Table(null, tableName), tokens[i]));
        }
        tables.put(tokens[0], cols);
      }
      br.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Gets path to file where a particular table is stored
   *
   * @param tableName table name
   * @return file where table is found on disk
   */
  public File getFileForTable(String tableName) {
    return new File(dbDirectory + "/data/" + tableName);
  }

  /**
   * Get columns for a table
   *
   * @param tableName table name
   * @return list of columns for the table
   */
  public ArrayList<Column> getColumns(String tableName) {
    return tables.get(tableName);
  }

  /**
   * Get columns with alias for a table
   *
   * @param table table object
   * @return list of columns for the table
   */
  public ArrayList<Column> getColumnsWithAlias(Table table) {
    ArrayList<Column> columns = tables.get(table.getName());
    ArrayList<Column> newColumns = new ArrayList<>();
    for (Column value : columns) {
      // Create a new Column object that include the input table (contains alias)
      Column column = new Column(table, value.getColumnName());
      newColumns.add(column);
    }
    return newColumns;
  }

  /**
   * Get join method & buffer page number from plan_builder_config.txt.
   *
   * <p>First row: 0: TNLJ, 1: BNLJ, 2: SMJ
   *
   * <p>Second row: 0: In-Memory Sort, 1: External Sort Only
   *
   * <p>if the join method is BNLJ, the buffer page number is needed.
   *
   * <p>if the sort method is External Sort, the buffer page number is needed.
   *
   * @param directory: The input directory.
   * @return void
   */
  public void setPlanBuilderConfig(String directory) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(directory));
      String line;

      // read first line
      line = br.readLine();
      String[] tokens = line.split("\\s");
      if (tokens.length > 1) {
        this.joinBufferPageNumber = Integer.parseInt(tokens[1]);
      }
      this.joinMethod = tokens[0];
      switch (tokens[0]) {
        case "0":
          this.joinMethod = "TNLJ";
          break;
        case "1":
          this.joinMethod = "BNLJ";
          break;
        case "2":
          this.joinMethod = "SMJ";
          break;
      }

      // read second line
      line = br.readLine();
      tokens = line.split("\\s");
      if (tokens.length > 1) {
        this.sortBufferPageNumber = Integer.parseInt(tokens[1]);
      }
      switch (tokens[0]) {
        case "0":
          this.sortMethod = "In-Memory Sort";
          break;
        case "1":
          this.sortMethod = "External Sort";
          break;
      }
      this.sortBufferPageNumber = Integer.parseInt(tokens[1]);

      br.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Get join method from plan_builder_config.txt.
   *
   * @return join method
   */
  public String getJoinMethod() {
    return this.joinMethod;
  }

  /**
   * Get join buffer page number from plan_builder_config.txt.
   *
   * @return join buffer page number
   */
  public int getJoinBufferPageNumber() {
    return this.joinBufferPageNumber;
  }

  /**
   * Get sort method from plan_builder_config.txt.
   *
   * @return sort method
   */
  public String getSortMethod() {
    return this.sortMethod;
  }

  /**
   * Get sort buffer page number from plan_builder_config.txt.
   *
   * @return sort buffer page number
   */
  public int getSortBufferPageNumber() {
    return this.sortBufferPageNumber;
  }
}
