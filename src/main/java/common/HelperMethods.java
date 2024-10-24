package common;

import java.util.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import physical_operator.Operator;

/** Helper methods for query plan builder */
public class HelperMethods {

  /**
   * Map column name to index as columnName : index
   *
   * @param columns list of columns
   * @return map of <column name, column index>
   */
  public static Map<String, Integer> mapColumnIndex(List<Column> columns) {
    Map<String, Integer> map = new HashMap<>();
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
   * @return list of all tables, if there is an alias of the same table, it will be treated as a
   *     separate table because alias will be treated as table names.
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
   * @param expression ComparisonOperator expression
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

  /**
   * Get a pair of column names from a comparison operator
   *
   * @param whereExpression expect to be a EqualsTo expression
   * @return pair of column names
   */
  public static Pair<Column, Column> getEqualityConditionColumnPair(Expression whereExpression) {
    ArrayList<ComparisonOperator> comparisons = flattenExpression(whereExpression);

    for (ComparisonOperator comparison : comparisons) {
      if (comparison instanceof EqualsTo) {
        Column leftColumn = comparison.getLeftExpression(Column.class);
        Column rightColumn = comparison.getRightExpression(Column.class);
        return new Pair<>(leftColumn, rightColumn);
      }
    }
    return null;
  }

  /**
   * A comparator that sorts the tuples based on the column specified in the orders list. Then by
   * the tuples based on the subsequent columns to break ties.
   *
   * @param orders list of columns
   * @param outputSchema list of all columns
   * @return a Comparator<T>
   * @param <T> tuple or Pair<?, Tuple>
   */
  public static <T> Comparator<T> getTupleComparator(
      List<Column> orders, List<Column> outputSchema) {
    Map<String, Integer> columnIndexMap = HelperMethods.mapColumnIndex(outputSchema);
    return new Comparator<T>() {
      @Override
      public int compare(T a, T b) {

        Tuple t1 = null, t2 = null;

        if (a instanceof Tuple && b instanceof Tuple) {
          t1 = (Tuple) a;
          t2 = (Tuple) b;
        }

        if (a instanceof Pair && b instanceof Pair) {
          t1 = ((Pair<?, Tuple>) a).getRight();
          t2 = ((Pair<?, Tuple>) b).getRight();
        }

        if (t1 == null && t2 == null) {
          return 0;
        } else if (t1 == null) {
          return 1;
        } else if (t2 == null) {
          return -1;
        }

        for (Column column : orders) {
          int index = columnIndexMap.get(column.getName(true));
          int compare = Integer.compare(t1.getElementAtIndex(index), t2.getElementAtIndex(index));

          // if the attributes are not equal, return the comparison result
          if (compare != 0) {
            return compare;
          }
        }

        // if the attributes are equal, traverse columnIndexMap to compare the next
        // non-equal column
        for (Column column : outputSchema) {
          String key = column.getName(true);
          if (t1.getElementAtIndex(columnIndexMap.get(key))
              != t2.getElementAtIndex(columnIndexMap.get(key))) {
            return Integer.compare(
                t1.getElementAtIndex(columnIndexMap.get(key)),
                t2.getElementAtIndex(columnIndexMap.get(key)));
          }
        }
        return 0;
      }
    };
  }
}
