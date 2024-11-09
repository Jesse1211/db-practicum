package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to contain information about database - names of tables, schema of each table and file
 * where each table is located. Uses singleton pattern.
 *
 * <p>Assumes dbDir has a schema.txt file and a /data subdirectory containing one file per relation,
 * named "relname".
 *
 * <p>Call by using DBCatalog.getInstance();
 */
public class DBCatalog {
  private final Logger logger = LogManager.getLogger();
  private final HashMap<String, ArrayList<Column>> tables;
  private static DBCatalog db;
  private final int bufferCapacity = 4096;

  // For Interpreter Config
  private String inputDir;
  private String tempDir;
  private String outputDir;
  private boolean isBuildIndex;
  private boolean isEvaluateSQL;

  // For Plan Builder Config
  private String joinMethod;
  private int joinBufferPageNumber = -1; // -1 means not set
  private String sortMethod;
  private int sortBufferPageNumber = -1; // -1 means not set
  private boolean useIndex = false;

  // For index information
  private Map<String, IndexInfo> indexInfo;

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
   * Read the configuration file and parse input directory, output directory, temporary directory,
   * isBuildIndex, and isEvaluateSQL.
   *
   * @param directory
   */
  public void setInterpreterConfig(String directory) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(directory));
      String line;
      int index = 0;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s");
        switch (index) {
          case 0:
            inputDir = tokens[0];
            setDataDirectory(inputDir + "/db/schema.txt");
            setPlanBuilderConfig(inputDir + "/plan_builder_config.txt");
            setIndexDirectory(inputDir + "/db/index_info.txt");
            break;
          case 1:
            outputDir = tokens[0];
            break;
          case 2:
            tempDir = tokens[0];
            break;
          case 3:
            isBuildIndex = tokens[0].equals("1");
            break;
          case 4:
            isEvaluateSQL = tokens[0].equals("1");
            break;
        }
        index++;
      }
      br.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /** Sets the data directory for the database catalog. */
  private void setDataDirectory(String directory) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(directory));
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
   * <p>Third row: 0: No Index, 1: Use Index
   *
   * <p>if not using index, then use the full-scan implementation
   *
   * <p>if using index, then parse info from index_info.txt and use the index implementation
   *
   * @param directory: The input directory.
   * @return void
   */
  private void setPlanBuilderConfig(String directory) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(directory));
      String line;
      String[] joinMethods = new String[] {"TNLJ", "BNLJ", "SMJ"};
      String[] sortMethods = new String[] {"In-Memory Sort", "External Sort"};

      // read first line
      line = br.readLine();
      String[] tokens = line.split("\\s");
      this.joinMethod = joinMethods[Integer.parseInt(tokens[0])];
      if (tokens.length > 1) {
        this.joinBufferPageNumber = Integer.parseInt(tokens[1]);
      }

      // read second line
      line = br.readLine();
      tokens = line.split("\\s");
      this.sortMethod = sortMethods[Integer.parseInt(tokens[0])];

      if (tokens.length > 1) {
        this.sortBufferPageNumber = Integer.parseInt(tokens[1]);
      }

      // read third line
      line = br.readLine();
      tokens = line.split("\\s");
      this.useIndex = tokens[0].equals("1");

      br.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Get & Set index information from index_info.txt.
   *
   * @return void
   */
  private void setIndexDirectory(String directory) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(directory));
      String line;
      indexInfo = new HashMap<>();
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s");
        String relationName = tokens[0];
        String attributeName = tokens[1];
        boolean isClustered = tokens[2].equals("1");
        int order = Integer.parseInt(tokens[3]);
        indexInfo.put(relationName, new IndexInfo(relationName, attributeName, isClustered, order));
      }
      br.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Get the input directory.
   *
   * @return input directory
   */
  public String getInputDir() {
    return inputDir;
  }

  /**
   * Get the output directory.
   *
   * @return output directory
   */
  public String getOutputDir() {
    return outputDir;
  }

  /**
   * Get the temporary directory.
   *
   * @return temporary directory
   */
  public String getTempDir() {
    return tempDir;
  }

  /**
   * Get the isBuildIndex flag: Flags to determine whether to build index
   *
   * <p>isBuildIndex = true: build indexes
   *
   * <p>isBuildIndex = false: not build indexes
   *
   * @return isBuildIndex flag
   */
  public boolean getIsBuildIndex() {
    return isBuildIndex;
  }

  /**
   * Get the isEvaluateSQL flag: Flags to determine whether to build evaluate SQL queries.
   *
   * <p>isEvaluateSQL = false: not run queries
   *
   * <p>isEvaluateSQL = true: run queries
   *
   * @return isEvaluateSQL flag
   */
  public boolean getIsEvaluateSQL() {
    return isEvaluateSQL;
  }

  /**
   * Gets path to file where a particular table is stored
   *
   * @param tableName table name
   * @return file where table is found on disk
   */
  public File getFileForTable(String tableName) {
    return new File(inputDir + "/db/data/" + tableName);
  }

  /**
   * Gets path to file where a particular index is stored
   *
   * @param tableName table name
   * @param attributeName attribute name
   * @return file where index is found on disk
   */
  public File getFileForIndex(String tableName, String attributeName) {
    return new File(inputDir + "/db/indexes/" + tableName + "." + attributeName);
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

  public String getTempDirectory() {
    return tempDir;
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

  public int getBufferCapacity() {
    return this.bufferCapacity;
  }

  /**
   * Get use index from plan_builder_config.txt.
   *
   * @return use index
   */
  public boolean getUseIndex() {
    return this.useIndex;
  }

  /**
   * Get index information
   *
   * @return IndexInfo
   */
  public IndexInfo getIndexInfo(String name) {
    return this.indexInfo.get(name);
  }

  public List<IndexInfo> getAllIndexInfo() {
    return this.indexInfo.values().stream().toList();
  }
}
