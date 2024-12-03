package common.stats;

import common.pair.Pair;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatsInfo {
  private List<String> list;
  public Map<String, Pair<Integer, Integer>> columnStats;
  public String tableName;
  public int count;

  public StatsInfo(String row) {
    this.list = Arrays.asList(row.split("\s"));
    this.columnStats = new HashMap<>();
    int index = 0;
    this.tableName = list.get(index++);
    this.count = Integer.parseInt(list.get(index++));
    while (index < list.size()) {
      addColumnStats(index++);
    }
  }

  private void addColumnStats(int i) {
    String cur = list.get(i);
    String[] curList = cur.split(",");
    String colName = curList[0];
    int min = Integer.parseInt(curList[1]);
    int max = Integer.parseInt(curList[2]);
    columnStats.put(colName, new Pair<>(min, max));
  }
}
