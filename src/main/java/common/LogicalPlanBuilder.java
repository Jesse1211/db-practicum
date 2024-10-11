package common;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
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

/**
 * Class to translate a JSQLParser statement into a relational algebra query
 * plan.
 */
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
      operatorNode = new SortOperatorNode(operatorNode, plainSelect.getOrderByElements());
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
   * @param table           the table to build the plan for
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
   * @param allTables       all tables in the query
   * @return the root of the query plan
   */
  private static OperatorNode buildMultiTablePlan(
      Expression whereExpression, ArrayList<Table> allTables) {
    ArrayList<ComparisonOperator> flattened = HelperMethods.flattenExpression(whereExpression);
    Map<String, Expression> tableWhereExpressionMap = new HashMap<>();
    Expression joinWhereExpression = null;
    Expression valueWhereExpression = null;

    /*
     * Separate the comparisons into 3 categories: value comparison, same-table
     * comparison, and join comparison
     */
    for (ComparisonOperator comparisonOperator : flattened) {

      // Get 2 table names from the comparison operator
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
      } else if (leftTableName == null
          || rightTableName == null
          || leftTableName.equals(rightTableName)) {
        // if same-table comparison or one table or both table name are the same, then
        // no join
        // needed.
        String tableName = leftTableName == null ? rightTableName : leftTableName;
        Expression expression = tableWhereExpressionMap.getOrDefault(tableName, null);
        if (expression == null) {
          tableWhereExpressionMap.put(tableName, comparisonOperator);
        } else {
          tableWhereExpressionMap.put(tableName, new AndExpression(expression, comparisonOperator));
        }
      } else {
        // two different tables, should join. join comparison
        if (joinWhereExpression == null) {
          joinWhereExpression = comparisonOperator;
        } else {
          joinWhereExpression = new AndExpression(joinWhereExpression, comparisonOperator);
        }
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

    // SELECT required column from each table individually and put into the queue
    ArrayDeque<OperatorNode> deque = new ArrayDeque<>();
    for (Table table : allTables) {
      OperatorNode operatorNode = new ScanOperatorNode(table);
      Alias alias = table.getAlias();
      String name = alias != null ? alias.getName() : table.getName();
      Expression expression = tableWhereExpressionMap.getOrDefault(name, null);
      if (expression != null) {
        // process same-table column comparisons.
        operatorNode = new SelectOperatorNode(operatorNode, expression);
      }
      deque.offer(operatorNode);
    }

    // Concatenate every pairs until 1 item left in queue.
    while (deque.size() > 1) {
      OperatorNode leftChildOperatorNode = deque.poll();
      OperatorNode rightChildOperatorNode = deque.poll();
      assert (leftChildOperatorNode != null && rightChildOperatorNode != null);
      OperatorNode operatorNode = new JoinOperatorNode(leftChildOperatorNode, rightChildOperatorNode);
      deque.addFirst(operatorNode); // stack back to the queue
    }

    OperatorNode operatorNode = deque.poll();
    assert (operatorNode != null);

    if (joinWhereExpression != null) {
      // if there is a join comparison, should evaluate.
      operatorNode = new SelectOperatorNode(operatorNode, joinWhereExpression);
    }
    return operatorNode;
  }
}
