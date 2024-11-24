package builder;

import common.tuple.Tuple;
import compiler.DBCatalog;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import physical_operator.Operator;
import physical_operator.ScanOperator;

public class StatsBuilder {
  HashMap<String, ArrayList<Column>> tables; // table name -> schema
  StringBuilder sb;

  /** Gather statistics from tables to stats.txt file. */
  public StatsBuilder() {
    this.tables = DBCatalog.getInstance().getTables();
    resetStatsFile();

    this.sb = new StringBuilder();
    tables.keySet().forEach((table) -> processTable(table));

    writeToStatsFile();
  }

  private void processTable(String table) {

    Operator scanOperator = new ScanOperator(tables.get(table).get(0).getTable());
    List<Column> tableSchema = tables.get(table);

    Map<String, int[]> columnStats = new HashMap<>(); // column name -> [min, max]

    for (Column column : tables.get(table)) {
      String colName = column.getColumnName();
      columnStats.put(colName, new int[] { Integer.MAX_VALUE, Integer.MIN_VALUE });
    }

    Tuple tuple;
    int numTuples = 0;
    int[] minArray = new int[tableSchema.size()];
    int[] maxArray = new int[tableSchema.size()];
    Arrays.fill(minArray, Integer.MAX_VALUE);
    Arrays.fill(maxArray, Integer.MIN_VALUE);

    while ((tuple = scanOperator.getNextTuple()) != null) {
      List<Integer> tupleList = tuple.getAllElements();
      numTuples++;

      for (int i = 0; i < tupleList.size(); i++) {
        int value = tupleList.get(i);
        minArray[i] = Math.min(minArray[i], value);
        maxArray[i] = Math.max(maxArray[i], value);
      }
    }

    // Write to stats.txt
    sb.append(table).append("\s");
    sb.append(numTuples).append("\s");

    for (int i = 0; i < tableSchema.size(); i++) {
      Column column = tableSchema.get(i);
      String colName = column.getColumnName();
      sb.append(colName).append(",");
      sb.append(minArray[i]).append(",");
      sb.append(maxArray[i]).append("\s");
    }
    sb.append("\n");
  }

  private void resetStatsFile() {
    try (FileWriter fileWriter = new FileWriter("src/test/resources/samples/input/db/stats.txt")) {
      fileWriter.write("");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void writeToStatsFile() {
    try (FileWriter fileWriter = new FileWriter("src/test/resources/samples/input/db/stats.txt")) {
      fileWriter.append(sb.toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
