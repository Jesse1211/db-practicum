package builder;

import common.ComparisonEvaluator;
import common.HelperMethods;
import common.UnionFindElement;
import common.pair.Pair;
import compiler.ExpressionEvaluator;
import java.util.ArrayDeque;
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
  public static OperatorNode buildPlan(Statement stmt) {
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
    return operatorNode;
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
  private static OperatorNode buildMultiTablePlan(Expression whereExpression, ArrayList<Table> allTables) {
    ArrayList<ComparisonOperator> flattened = HelperMethods.flattenExpression(whereExpression);

    // use to evaluate join operators using union find
    ComparisonEvaluator comparisonEvaluator = new ComparisonEvaluator();
    Expression valueWhereExpression = null;

    for (ComparisonOperator comparisonOperator : flattened) {
      Pair<String, String> tableNamePair = HelperMethods.getComparisonTableNames(comparisonOperator);
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
      }else{
        comparisonEvaluator.visit(comparisonOperator);
      }
    }

    // evaluate value comparisons like 42 = 42 or 21 > 40. If it's false, the result
    // will be empty
    // */
    if (valueWhereExpression != null) {
      ExpressionEvaluator evaluator = new ExpressionEvaluator();
      valueWhereExpression.accept(evaluator);
      if (evaluator.getResult() == false) {
        return new EmptyOperatorNode();
      }
    }

    Set<UnionFindElement> unionFindElements = comparisonEvaluator.getResult();
    Set<ComparisonOperator> residuals = comparisonEvaluator.getResiduals();
    Map<String, ComparisonOperator> notEqualToMap = comparisonEvaluator.getNotEqualToValueMap();

    //regroup unionFindElements by table
    List<OperatorNode> joinChildren = new ArrayList<>();
    List<String> tableNames = new ArrayList<>();
    for (Table table : allTables){

      OperatorNode operatorNode = new ScanOperatorNode(table);
      Alias alias = table.getAlias();
      String tableName = alias != null ? alias.getName() : table.getName();
      tableNames.add(tableName);

      Expression expression = createComparisonExpressionForTable(unionFindElements, tableName);
      if (notEqualToMap.containsKey(tableName)) {
        if (expression == null) {
          expression = notEqualToMap.get(tableName);
        }else{
          expression = new AndExpression(expression, notEqualToMap.get(tableName));
        }
      }
      if (expression != null) {
        operatorNode = new SelectOperatorNode(operatorNode, expression);
      }
      joinChildren.add(operatorNode);
    }

    return new JoinOperatorNode(tableNames, joinChildren,residuals);

//
////
////    Map<Pair<String, String>, Expression> joinWhereExpressionMap = new HashMap<>();
//
//
//    /*
//     * Separate the comparisons into 3 categories: value comparison, same-table
//     * comparison, and join comparison
//     */
//    for (ComparisonOperator comparisonOperator : flattened) {
//
//      // Get 2 table names from the comparison operator
//      Pair<String, String> tableNamePair =
//          HelperMethods.getComparisonTableNames(comparisonOperator);
//      String leftTableName = tableNamePair.getLeft();
//      String rightTableName = tableNamePair.getRight();
//
//      if (leftTableName == null && rightTableName == null) {
//        // if value comparison
//        // both are values, ex: 42 = 42, should evaluate first.
//        if (valueWhereExpression == null) {
//          valueWhereExpression = comparisonOperator;
//        } else {
//          valueWhereExpression = new AndExpression(valueWhereExpression, comparisonOperator);
//        }
//      } else if (leftTableName == null
//          || rightTableName == null
//          || leftTableName.equals(rightTableName)) {
//        // if same-table comparison or one table or both table name are the same, then
//        // no join
//        // needed.
//        String tableName = leftTableName == null ? rightTableName : leftTableName;
//        Expression expression = tableWhereExpressionMap.getOrDefault(tableName, null);
//        if (expression == null) {
//          tableWhereExpressionMap.put(tableName, comparisonOperator);
//        } else {
//          tableWhereExpressionMap.put(tableName, new AndExpression(expression, comparisonOperator));
//        }
//      } else {
//        // two different tables, put i  t in joinWhereExpression
//        Expression expression = joinWhereExpressionMap.getOrDefault(tableNamePair, null);
//        if (expression == null) {
//          joinWhereExpressionMap.put(tableNamePair, comparisonOperator);
//        } else {
//          joinWhereExpressionMap.put(
//              tableNamePair, new AndExpression(expression, comparisonOperator));
//        }
//      }
//    }
//
//    // evaluate value comparisons like 42 = 42 or 21 > 40. If it's false, the result
//    // will be empty
//    // */
//    if (valueWhereExpression != null) {
//      ExpressionEvaluator evaluator = new ExpressionEvaluator();
//      valueWhereExpression.accept(evaluator);
//      if (evaluator.getResult() == false) {
//        return new EmptyOperatorNode();
//      }
//    }
//
//    // SELECT required column from each table individually and put into the queue
//    ArrayDeque<Pair<String, OperatorNode>> deque = new ArrayDeque<>();
//    for (Table table : allTables) {
//      OperatorNode operatorNode = new ScanOperatorNode(table);
//      Alias alias = table.getAlias();
//      String name = alias != null ? alias.getName() : table.getName();
//      Expression expression = tableWhereExpressionMap.getOrDefault(name, null);
//      if (expression != null) {
//        // process same-table column comparisons.
//        // Where Sailer.A < 50 AND Sailer.A > 30
//        operatorNode = new SelectOperatorNode(operatorNode, expression);
//      }
//      deque.offer(new Pair<>(name, operatorNode));
//    }
//
//    List<String> names = new ArrayList<>();
//    // Concatenate every pairs until 1 item left in queue.
//    while (deque.size() > 1) {
//      Pair<String, OperatorNode> leftPair = deque.poll();
//      Pair<String, OperatorNode> rightPair = deque.poll();
//      assert (leftPair != null && rightPair != null);
//      OperatorNode operatorNode = new JoinOperatorNode(leftPair.getRight(), rightPair.getRight());
//      // if left is a single table selection (not a join node), usually happens during first
//      // iteration, add to the list.
//      if (leftPair.getLeft() != null) {
//        names.add(leftPair.getLeft());
//      }
//
//      String rightName = rightPair.getLeft();
//      assert (rightName != null);
//
//      // for each of name in names arr, concat all expressions that have tables of name and
//      // rightname
//      Expression expression = null;
//      for (String name : names) {
//        Expression _expression =
//            joinWhereExpressionMap.getOrDefault(new Pair<>(name, rightName), null);
//        if (_expression == null) continue;
//
//        if (expression == null) {
//          expression = _expression;
//        } else {
//          expression = new AndExpression(expression, _expression);
//        }
//      }
//
//      names.add(rightName);
//      if (expression != null) {
//        operatorNode = new SelectOperatorNode(operatorNode, expression);
//      }
//      deque.addFirst(new Pair<>(null, operatorNode)); // stack back to the queue
//    }
//
//    OperatorNode operatorNode = deque.poll().getRight();
//    return operatorNode;
  }



  private static Expression createComparisonExpressionForTable(Set<UnionFindElement> unionFindElements, String tableName){
    Expression expression = null;
    // for find all columns in union find groups that have this table. n^2 complexity
    for (UnionFindElement element : unionFindElements){
      for(Entry<String, Column> entry: element.attributes.entrySet()){
        Column column = entry.getValue();
        if(column.getTable().getName().equals(tableName)){

          if (element.lowerBound == Integer.MIN_VALUE && element.upperBound == Integer.MAX_VALUE){
            //then we just need a regular scan
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
