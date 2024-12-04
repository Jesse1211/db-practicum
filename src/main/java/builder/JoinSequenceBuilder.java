package builder;

import common.UnionFind;
import common.UnionFindElement;
import common.pair.Pair;
import common.stats.StatsInfo;
import compiler.DBCatalog;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator_node.JoinOperatorNode;
import operator_node.OperatorNode;

public class JoinSequenceBuilder {

  /** An object to store state for a join combination */
  class Order {
    private List<Table> tableCombination; // order matters, 1st means root, last means leaf
    List<Expression> joinConditions;
    private double cost;

    /**
     * Build a object which stores the join order and cost
     *
     * @param tableCombination order of the tables, last means root, 1st means leaf
     * @param joinConditions join conditions
     * @param cost total cost of the join (from leaf (last) to root (first))
     */
    public Order(List<Table> tableCombination, List<Expression> joinConditions, double cost) {
      this.tableCombination = tableCombination;
      this.joinConditions = joinConditions;
      this.cost = cost;
    }
  }

  Map<String, OperatorNode> tableNameToNode;
  List<Table> tableList;

  /**
   * Dynamic programming table to store the cost of joining two nodes. dp[i][j] = the best cost of
   * joining from the ith node to the jth node. the last row represents the root node. base case:
   * dp[0][j] = the cost of single relation of the jth node is Zero.
   */
  Order[][] dp;

  public JoinSequenceBuilder(JoinOperatorNode joinNode) {
    this.tableList = joinNode.getTables();
    this.tableNameToNode = new HashMap<>();

    for (int i = 0; i < this.tableList.size(); i++) {
      Table table = tableList.get(i);
      Alias alias = table.getAlias();
      String aliasName = alias != null ? alias.getName() : table.getName();
      this.tableNameToNode.put(aliasName, joinNode.getChildNodes().get(i));
    }

    this.dp = new Order[tableNameToNode.size()][tableNameToNode.size()];
    computeDpTable();
  }

  /**
   * Compute the dynamic programming table to store the cost of joining two nodes. Similar logic to
   * LC.77 Combinations.
   */
  private void computeDpTable() {
    // 1. Initialize the base case
    for (int i = 0; i < tableList.size(); i++) {
      // cost only counts the intermediate relations, hence 0 for the base case
      dp[0][i] = new Order(new ArrayList<>(List.of(this.tableList.get(i))), new ArrayList<>(), 0);
    }

    // 2. Fill the dp table
    for (int i = 1; i < tableList.size(); i++) { // exclude base case
      for (int j = 0; j < tableList.size(); j++) { // traverse every column (from leaf to root)
        double minCost = Double.MAX_VALUE;
        Table tableMatch = null;
        Order prev = dp[i - 1][j];

        // Find the best cost of joining the ith node to the jth node
        for (int k = 0; k < tableList.size(); k++) {
          Table curTable = tableList.get(k);
          if (prev.tableCombination.contains(curTable)) {
            continue;
          }

          double cost = computeCost(prev, curTable.getName());

          if (cost < minCost) {
            tableMatch = curTable;
            minCost = cost;
          }
        }

        minCost += prev.cost; // the cost of left child + the cost of joining new table
        List<Table> tableCombination = new ArrayList<>();
        tableCombination.addAll(prev.tableCombination);
        tableCombination.add(tableMatch);

        List<Expression> joinConditions = new ArrayList<>();
        joinConditions.addAll(prev.joinConditions);
        dp[i][j] = new Order(tableCombination, joinConditions, minCost);
      }
    }
  }

  /**
   * Compute all join cost from the table sequence
   *
   * @param order current order
   * @param curTable new table to join the order
   * @return the cost after join
   */
  private double computeCost(Order order, String newTable) {
    double joinCost = 1.0;
    Set<UnionFindElement> unionFind = UnionFind.getInstance(false).getElements();

    // Cost is the intermediate relations' tuples, hence the product of the tuples
    for (Table table : order.tableCombination) {
      String tableName = table.getName();
      StatsInfo statsInfo = DBCatalog.getInstance().getStatsInfo(tableName);
      joinCost *= statsInfo.count;
    }

    // Compute v-value boundry for each table
    Map<String, Pair<Integer, Integer>> tableBound = computeVValue(unionFind);

    for (UnionFindElement element : unionFind) {

      Set<String> tableUnion = new HashSet<>(); // avoid duplicate table combination
      for (Column column1 : element.attributes.values()) {
        Table table1 = column1.getTable();
        String i = column1.getTable().getName();

        for (Column column2 : element.attributes.values()) {
          Table table2 = column2.getTable();
          String j = column2.getTable().getName();
          if (!tableUnion.contains(i + j) && !tableUnion.contains(j + i)) {
            tableUnion.add(i + j);
            tableUnion.add(j + i);

            if (tableList.contains(table1) && tableList.contains(table2)) {
              double v1 = findVValue(column1, tableBound);
              double v2 = findVValue(column2, tableBound);
              joinCost *= joinCost / Math.max(v1, v2);
            }
          }
        }
      }
    }

    return joinCost;
  }

  /**
   * Compute the v-value boundry for each table
   *
   * @param unionFind union find set
   * @return table boundry
   */
  private Map<String, Pair<Integer, Integer>> computeVValue(Set<UnionFindElement> unionFind) {
    Map<String, Pair<Integer, Integer>> tableBound = new HashMap<>(); // table -> <min, max>
    for (UnionFindElement element : unionFind) {
      for (Column column : element.attributes.values()) {
        String tableName = column.getTable().getName();
        Pair<Integer, Integer> pair =
            tableBound.getOrDefault(tableName, new Pair<>(Integer.MAX_VALUE, Integer.MIN_VALUE));
        tableBound.put(
            tableName,
            new Pair<>(
                Math.min(pair.getLeft(), element.lowerBound),
                Math.max(pair.getRight(), element.upperBound)));
      }
    }
    return tableBound;
  }

  /**
   * Find the v-value for a column
   *
   * @param A column
   * @param tableBound table boundry
   * @return v-value
   */
  private int findVValue(Column A, Map<String, Pair<Integer, Integer>> tableBound) {
    String tableName = A.getTable().getName();
    StatsInfo statsInfo = DBCatalog.getInstance().getStatsInfo(tableName);
    int max =
        Math.max(
            statsInfo.columnStats.get(A.getColumnName()).getRight(),
            tableBound.get(tableName).getRight());
    int min =
        Math.min(
            statsInfo.columnStats.get(A.getColumnName()).getLeft(),
            tableBound.get(tableName).getLeft());

    if (max - min + 1 == 0) {
      return 1;
    }
    return max - min + 1;
  }

  /**
   * Get the best join order
   *
   * @return the best join order
   */
  public ArrayDeque<OperatorNode> getJoinOrder() {

    // 3. Find the best cost of joining all the nodes
    double minCost = Double.MAX_VALUE;
    Order bestOrder = dp[tableList.size() - 1][0];
    for (int i = 1; i < tableList.size(); i++) {
      Order cur = dp[tableList.size() - 1][i];
      if (cur.cost < minCost) {
        minCost = cur.cost;
        bestOrder = cur;
      }
    }

    // convert the table order sequence to a list of operator node
    ArrayDeque<OperatorNode> joinOrder = new ArrayDeque<>();

    for (int i = 0; i < bestOrder.tableCombination.size(); i++) {
      Table table = bestOrder.tableCombination.get(i);
      Alias alias = table.getAlias();
      String aliasName = alias != null ? alias.getName() : table.getName();
      joinOrder.add(tableNameToNode.get(aliasName));
    }

    return joinOrder;
  }
}
