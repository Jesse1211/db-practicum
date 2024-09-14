package common;

import java.util.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import operator.Operator;

/** Helper methods for query plan builder */
public class HelperMethods {
  /**
   * Map column name to index as columnName : index
   *
   * @param columns
   * @return
   */
  public static Map<String, Integer> mapColumnIndex(ArrayList<Column> columns) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (int i = 0; i < columns.size(); i++) {
      map.put(columns.get(i).getName(true), i);
    }
    return map;
  }

  /**
   * Collect all tuples from the operator
   *
   * @param operator operator
   * @return list of tuples
   */
  public static List<Tuple> getAllTuples(Operator operator) {
    Tuple tuple;
    List<Tuple> tuples = new ArrayList<>();
    while ((tuple = operator.getNextTuple()) != null) {
      tuples.add(tuple);
    }
    return tuples;
  }

  /**
   * Store all tables in from statement, including join tables
   *
   * @param table main table
   * @param joins list of joins
   * @return list of all tables
   */
  public static ArrayList<Table> getAllTables(Table table, List<Join> joins) {
    ArrayList<Table> allTables = new ArrayList<>();
    allTables.add(table);
    if (joins != null) {
      for (Join join : joins) {
        allTables.add((Table) join.getRightItem());
      }
    }
    return allTables;
  }

  /**
   * Flatten the expression into a list of comparison operators
   *
   * @param expression expression
   * @return list of comparison operators
   */
  public static ArrayList<ComparisonOperator> flattenExpression(Expression expression) {
    ArrayList<ComparisonOperator> expressions = new ArrayList<>();
    if (expression instanceof ComparisonOperator) {
      expressions.add((ComparisonOperator) expression);
      return expressions;
    }

    if (expression instanceof BinaryExpression) {
      Expression leftExpression = ((BinaryExpression) expression).getLeftExpression();
      Expression rightExpression = ((BinaryExpression) expression).getRightExpression();
      expressions.addAll(flattenExpression(leftExpression));
      expressions.addAll(flattenExpression(rightExpression));
    }
    return expressions;
  }

  /**
   * Get a pair of table names from a comparison operator
   *
   * @param expression
   * @return pair of table names
   */
  public static Pair<String, String> getComparisonTableNames(ComparisonOperator expression) {
    Expression leftExpression = expression.getLeftExpression();
    Expression rightExpression = expression.getRightExpression();
    String leftTableName = null;
    String rightTableName = null;
    if (leftExpression instanceof Column) {
      leftTableName = ((Column) leftExpression).getTable().getName();
    }

    if (rightExpression instanceof Column) {
      rightTableName = ((Column) rightExpression).getTable().getName();
    }

    return new Pair<>(leftTableName, rightTableName);
  }

  @Deprecated
  /** not used anymore */
  public static Map<String, Table> buildTableNameMap(ArrayList<Table> allTables) {
    Map<String, Table> tableNameMap = new HashMap<>(); // alias to name

    for (Table table : allTables) {
      tableNameMap.put(table.getName(), table);
      if (table.getAlias() != null) {
        tableNameMap.put(table.getAlias().getName(), table);
      }
    }
    return tableNameMap;
  }
}
