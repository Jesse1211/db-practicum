package builder;

import common.ComparisonEvaluator;
import common.HelperMethods;
import common.UnionFindElement;
import common.index.IndexDeserializer;
import common.index.IndexInfo;
import common.pair.Pair;
import common.stats.StatsInfo;
import compiler.DBCatalog;
import compiler.ExpressionEvaluator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operator_node.DuplicateEliminationOperatorNode;
import operator_node.EmptyOperatorNode;
import operator_node.JoinOperatorNode;
import operator_node.OperatorNode;
import operator_node.ProjectOperatorNode;
import operator_node.ScanOperatorNode;
import operator_node.SelectOperatorNode;
import operator_node.SortOperatorNode;

/** Class to translate a JSQLParser statement into a relational algebra query plan. */
public class LogicalPlanBuilder {

  /**
   * Top level method to translate statement to query plan
   *
   * @param stmt query to be translated
   * @return the root of the query plan
   */
  public static Pair<OperatorNode, StringBuilder> buildPlan(Statement stmt) {
    // https://github.com/JSQLParser/JSqlParser/wiki/Examples-of-SQL-parsing
    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    Table table = (Table) plainSelect.getFromItem();
    List<Join> joins = plainSelect.getJoins();
    Expression whereExpression = plainSelect.getWhere();
    ArrayList<Table> allTables = HelperMethods.getAllTables(table, joins);

    OperatorNode operatorNode;
    if (joins != null) {
      operatorNode = buildMultiTablePlan(whereExpression, allTables);
    } else {
      operatorNode = buildSingleTablePlan(whereExpression, table);
    }

    // select * does not require projection, we can skip that.
    if (plainSelect.getSelectItems().size() > 1
        || !(plainSelect.getSelectItems().getFirst() instanceof AllColumns)) {
      operatorNode = new ProjectOperatorNode(operatorNode, plainSelect.getSelectItems());
    }

    if (plainSelect.getOrderByElements() != null) {
      List<Column> orders =
          plainSelect.getOrderByElements().stream()
              .map((orderByElement) -> (Column) orderByElement.getExpression())
              .toList();
      operatorNode = new SortOperatorNode(operatorNode, orders);
    }

    if (plainSelect.getDistinct() != null) {
      operatorNode = new DuplicateEliminationOperatorNode(operatorNode);
    }

    return new Pair<>(operatorNode, operatorNode.print());
  }

  /**
   * Build a query plan for a single table
   *
   * @param whereExpression the where expression from the query
   * @param table the table to build the plan for
   * @return the root of the query plan
   */
  private static OperatorNode buildSingleTablePlan(Expression whereExpression, Table table) {
    OperatorNode operatorNode = new ScanOperatorNode(table);
    if (whereExpression != null) {
      operatorNode = new SelectOperatorNode(operatorNode, whereExpression);
    }
    return operatorNode;
  }

  /**
   * Build a query plan for multiple tables (joins)
   *
   * @param whereExpression the where expression from the query
   * @param allTables all tables in the query
   * @return the root of the query plan
   */
  private static OperatorNode buildMultiTablePlan(
      Expression whereExpression, ArrayList<Table> allTables) {
    ArrayList<ComparisonOperator> flattened = HelperMethods.flattenExpression(whereExpression);

    // use to evaluate join operators using union find
    ComparisonEvaluator comparisonEvaluator = new ComparisonEvaluator();
    Expression valueWhereExpression = null;

    for (ComparisonOperator comparisonOperator : flattened) {
      Pair<String, String> tableNamePair =
          HelperMethods.getComparisonTableNames(comparisonOperator);
      String leftTableName = tableNamePair.getLeft();
      String rightTableName = tableNamePair.getRight();

      if (leftTableName == null && rightTableName == null) {
        // if value comparison
        // both are values, ex: 42 = 42, should evaluate first.
        if (valueWhereExpression == null) {
          valueWhereExpression = comparisonOperator;
        } else {
          valueWhereExpression = new AndExpression(valueWhereExpression, comparisonOperator);
        }
      } else {
        comparisonEvaluator.visit(comparisonOperator);
      }
    }

    // evaluate value comparisons like 42 = 42 or 21 > 40. If it's false, the result
    // will be empty
    // */
    if (valueWhereExpression != null) {
      ExpressionEvaluator evaluator = new ExpressionEvaluator();
      valueWhereExpression.accept(evaluator);
      if (!evaluator.getResult()) {
        return new EmptyOperatorNode();
      }
    }

    Set<UnionFindElement> unionFindElements = comparisonEvaluator.getResult();
    Set<ComparisonOperator> residuals = comparisonEvaluator.getResiduals();
    Map<String, ComparisonOperator> notEqualToMap = comparisonEvaluator.getNotEqualToValueMap();
    Set<Pair<String, String>> equalityJoinMap = comparisonEvaluator.getEqualityJoinMap();

    // regroup unionFindElements by table
    List<OperatorNode> joinChildren = new ArrayList<>();
    List<String> aliasNames = new ArrayList<>();
    for (Table table : allTables) {
      Alias alias = table.getAlias();
      String aliasName = alias != null ? alias.getName() : table.getName();
      aliasNames.add(aliasName);

      String index = chooseScanIndex(unionFindElements, table);
      Expression expression =
          createComparisonExpressionForTable(unionFindElements, aliasName, index);

      OperatorNode operatorNode;
      if (index != null) {
        Pair<Integer, Integer> indexBounds = getBounds(unionFindElements, aliasName + "." + index);
        operatorNode =
            new ScanOperatorNode(table, index, indexBounds.getLeft(), indexBounds.getRight());
      } else {
        operatorNode = new ScanOperatorNode(table);
      }

      if (notEqualToMap.containsKey(aliasName)) {
        if (expression == null) {
          expression = notEqualToMap.get(aliasName);
        } else {
          expression = new AndExpression(expression, notEqualToMap.get(aliasName));
        }
      }

      if (expression != null) {
        operatorNode = new SelectOperatorNode(operatorNode, expression);
      }
      joinChildren.add(operatorNode);
    }

    return new JoinOperatorNode(allTables, joinChildren, residuals, equalityJoinMap);
  }

  /**
   * Get the lower, upper bounds for a given attribute
   *
   * @param unionFindElements set of union find elements
   * @param attributeName attribute name to get bounds for
   * @return pair of lower, upper bounds
   */
  private static Pair<Integer, Integer> getBounds(
      Set<UnionFindElement> unionFindElements, String attributeName) {
    for (UnionFindElement element : unionFindElements) {
      if (element.attributes.containsKey(attributeName)) {
        return new Pair<>(element.lowerBound, element.upperBound);
      }
    }
    return null;
  }

  /**
   * Choose the best index to scan for a given table
   *
   * @param unionFindElements set of union find elements
   * @param table table to choose index for
   * @return the index to scan
   */
  private static String chooseScanIndex(Set<UnionFindElement> unionFindElements, Table table) {
    String tableName = table.getName();
    Map<String, Double> reductionFactor = computeReductionFactor(unionFindElements, table);
    IndexInfo indexInfo = DBCatalog.getInstance().getIndexInfo(tableName);
    if (indexInfo == null) return null;
    StatsInfo statsInfo = DBCatalog.getInstance().getStatsInfo(tableName);
    int numTuples = statsInfo.count;
    int numPages =
        Math.ceilDiv(
            numTuples * statsInfo.columnStats.size() * 4,
            DBCatalog.getInstance().getBufferCapacity());

    double minCost = numPages;
    String selectedIndex = null;
    for (Entry<String, Double> entry : reductionFactor.entrySet()) {
      String attribute = entry.getKey();
      Double r = entry.getValue();

      // if no index on current column, continue
      if (!indexInfo.attributes.containsKey(attribute)) continue;
      double cost;
      // If p is the number of pages in the relation, t the number of tuples, r the reduction factor
      // and l the
      // number of leaves in the index, the cost for a clustered index is 3 + p ∗ r while for an
      // unclustered index it is
      // 3 + l ∗ r + t ∗ r.
      boolean isClustered = indexInfo.attributes.get(attribute).getLeft();
      if (isClustered) {
        cost = 0 + numPages * r;
      } else {
        int numLeaves = IndexDeserializer.getNumLeaves(indexInfo.relationName, attribute);
        cost = 0 + numLeaves * r + numTuples * r;
      }

      if (cost < minCost) {
        minCost = cost;
        selectedIndex = attribute;
      }
    }
    return selectedIndex;
  }

  /**
   * Compute the reduction factor for a given table
   *
   * @param unionFindElements set of union find elements
   * @param table table to compute reduction factor for
   * @return map of column name to reduction factor
   */
  private static Map<String, Double> computeReductionFactor(
      Set<UnionFindElement> unionFindElements, Table table) {
    Alias alias = table.getAlias();
    String aliasName = alias != null ? alias.getName() : table.getName();
    String tableName = table.getName();

    StatsInfo info = DBCatalog.getInstance().getStatsInfo(tableName);
    Map<String, Double> reductionFactors = new HashMap<>();
    for (UnionFindElement element : unionFindElements) {
      for (Entry<String, Column> entry : element.attributes.entrySet()) {
        Column column = entry.getValue();
        if (column.getTable().getName().equals(aliasName)) {
          String columnName = column.getColumnName();
          Pair<Integer, Integer> tupleBound = info.columnStats.get(columnName);
          long upperBound = Math.min(element.upperBound, tupleBound.getRight());
          long lowerBound = Math.max(element.lowerBound, tupleBound.getLeft());
          long totalRange = tupleBound.getRight() - tupleBound.getLeft();
          reductionFactors.put(columnName, (upperBound - lowerBound) / (double) totalRange);
        }
      }
    }
    return reductionFactors;
  }

  /**
   * Create a comparison expression for a given table
   *
   * @param unionFindElements set of union find elements
   * @param tableName table to create comparison expression for
   * @param index index to scan
   * @return the comparison expression
   */
  private static Expression createComparisonExpressionForTable(
      Set<UnionFindElement> unionFindElements, String tableName, String index) {
    Expression expression = null;
    // for find all columns in union find groups that have this table. n^2 complexity
    for (UnionFindElement element : unionFindElements) {
      for (Entry<String, Column> entry : element.attributes.entrySet()) {
        Column column = entry.getValue();
        // no need to build comparisons for index since we are using index scan, that'll select the
        // values
        // between lower bound and upper bound.
        if (column.getColumnName().equals(index)) continue;
        if (column.getTable().getName().equals(tableName)) {

          if (element.lowerBound == Integer.MIN_VALUE && element.upperBound == Integer.MAX_VALUE) {
            // then we just need a regular scan
            continue;
          }

          ComparisonOperator greaterThanEquals = new GreaterThanEquals();
          greaterThanEquals.setLeftExpression(column);
          greaterThanEquals.setRightExpression(new LongValue(element.lowerBound));

          ComparisonOperator minorThanEquals = new MinorThanEquals();
          minorThanEquals.setLeftExpression(column);
          minorThanEquals.setRightExpression(new LongValue(element.upperBound));
          Expression comparisonOperator = new AndExpression(greaterThanEquals, minorThanEquals);

          if (expression == null) {
            expression = comparisonOperator;
          } else {
            expression = new AndExpression(expression, comparisonOperator);
          }
        }
      }
    }
    return expression;
  }
}
