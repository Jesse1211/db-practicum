package common;

import java.util.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import physical_operator.Operator;

/** Helper methods for query plan builder */
public class HelperMethods {

  /**
   * Map column name to index as columnName : index Default will use alias
   *
   * @param columns list of columns
   * @return map of <column name, column index>
   */
  public static Map<String, Integer> mapColumnIndex(List<Column> columns) {
    return mapColumnIndex(columns, true);
  }

  /**
   * Map column name to index as columnName : index
   *
   * @param columns list of columns
   * @param useAlias useAlias to parse
   * @return
   */
  public static Map<String, Integer> mapColumnIndex(List<Column> columns, boolean useAlias) {
    Map<String, Integer> map = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      map.put(columns.get(i).getName(useAlias), i);
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

  public static Expression getNonIndexedComparisons(
      List<ComparisonOperator> comparisons, List<ComparisonOperator> indexedComparisons) {
    Expression expression = null;
    for (ComparisonOperator comparison : comparisons) {
      if (!indexedComparisons.contains(comparison)) {
        if (expression == null) {
          expression = comparison;
        } else {
          expression = new AndExpression(expression, comparison);
        }
      }
    }
    return expression;
  }

  public static List<ComparisonOperator> getIndexedComparisons(
      List<ComparisonOperator> comparisons, Table table) {
    List<ComparisonOperator> indexedComparisons = new ArrayList<>();
    IndexInfo indexInfo = DBCatalog.getInstance().getIndexInfo(table.getName());
    if (indexInfo == null) {
      return indexedComparisons;
    }
    for (ComparisonOperator comparison : comparisons) {
      // Only these comparisons can be used with indexes
      if (isComparisonIndexed(comparison, indexInfo)
          && (comparison instanceof EqualsTo
              || comparison instanceof GreaterThan
              || comparison instanceof GreaterThanEquals
              || comparison instanceof MinorThan
              || comparison instanceof MinorThanEquals)) {
        indexedComparisons.add(comparison);
      }
    }
    return indexedComparisons;
  }

  private static boolean isComparisonIndexed(ComparisonOperator comparison, IndexInfo indexInfo) {
    Expression leftExpression = comparison.getLeftExpression();
    Expression rightExpression = comparison.getRightExpression();

    if (leftExpression instanceof Column) {
      String columnName = ((Column) leftExpression).getColumnName();
      return columnName.equals(indexInfo.attributeName);
    }

    if (rightExpression instanceof Column) {
      String columnName = ((Column) rightExpression).getColumnName();
      return columnName.equals(indexInfo.attributeName);
    }
    return false;
  }

  // First element in the pair represents the number is <= or = or >= using -1, 0, 1 respectively
  public static Pair<Integer, Integer> getComparisonValue(ComparisonOperator comparison) {
    Expression leftExpression = comparison.getLeftExpression();
    Expression rightExpression = comparison.getRightExpression();

    if (leftExpression instanceof LongValue) {
      return new Pair<>(-1, (int) ((LongValue) leftExpression).getValue());
    } else {
      return new Pair<>(1, (int) ((LongValue) rightExpression).getValue());
    }
  }

  public static Pair<Integer, Integer> getLowKeyHighKey(
      List<ComparisonOperator> indexedComparisons) {
    int lowKey = Integer.MIN_VALUE;
    int highKey = Integer.MAX_VALUE;

    for (ComparisonOperator comparison : indexedComparisons) {
      Pair<Integer, Integer> pair = getComparisonValue(comparison);
      int side = pair.getLeft();
      int value = pair.getRight();

      if (comparison instanceof GreaterThan) {
        if (side == 1) {
          // x > value
          lowKey = Math.max(lowKey, value + 1);
        } else {
          // value > x => x < value
          highKey = Math.min(highKey, value - 1);
        }
      } else if (comparison instanceof GreaterThanEquals) {
        if (side == 1) {
          // x >= value
          lowKey = Math.max(lowKey, value);
        } else {
          // value >= x => x <= value
          highKey = Math.min(highKey, value);
        }
      } else if (comparison instanceof MinorThan) {
        if (side == 1) {
          // x < value
          highKey = Math.min(highKey, value - 1);
        } else {
          // value < x => x > value
          lowKey = Math.max(lowKey, value + 1);
        }
      } else if (comparison instanceof MinorThanEquals) {
        if (side == 1) {
          // x <= value
          highKey = Math.min(highKey, value);
        } else {
          // value <= x => x >= value
          lowKey = Math.max(lowKey, value);
        }
      } else if (comparison instanceof EqualsTo) {
        // x = value or value = x
        lowKey = highKey = value;
        return new Pair<>(lowKey, highKey);
      }
    }
    return new Pair<>(lowKey, highKey);
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
  public static Pair<Column, Column> getEqualityConditionColumnPair(
      Expression whereExpression, Operator leftOperator, Operator rightOperator) {
    ArrayList<ComparisonOperator> comparisons = flattenExpression(whereExpression);

    for (ComparisonOperator comparison : comparisons) {
      if (comparison instanceof EqualsTo) {
        Column leftColumn = comparison.getLeftExpression(Column.class);
        Column rightColumn = comparison.getRightExpression(Column.class);

        // we need the map to distinguish left and right
        Map<String, Integer> leftMap = HelperMethods.mapColumnIndex(leftOperator.getOutputSchema());
        Map<String, Integer> rightMap =
            HelperMethods.mapColumnIndex(rightOperator.getOutputSchema());

        if (leftMap.containsKey(leftColumn.getName(true))
            && rightMap.containsKey(rightColumn.getName(true))) {
          return new Pair<>(leftColumn, rightColumn);
        }

        if (leftMap.containsKey(rightColumn.getName(true))
            && rightMap.containsKey(leftColumn.getName(true))) {
          return new Pair<>(rightColumn, leftColumn);
        }
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
   * @param <T> tuple or Pair<?, Tuple>
   * @return a Comparator<T>
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
