package common;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
import operator.*;

/**
 * Class to translate a JSQLParser statement into a relational algebra query plan. For now only
 * works for Statements that are Selects, and specifically PlainSelects. Could implement the visitor
 * pattern on the statement, but doesn't for simplicity as we do not handle nesting or other complex
 * query features.
 *
 * <p>Query plan fixes join order to the order found in the from clause and uses a left deep tree
 * join. Maximally pushes selections on individual relations and evaluates join conditions as early
 * as possible in the join tree. Projections (if any) are not pushed and evaluated in a single
 * projection operator after the last join. Finally, sorting and duplicate elimination are added if
 * needed.
 *
 * <p>For the subset of SQL which is supported as well as assumptions on semantics, see the Project
 * 2 student instructions, Section 2.1
 */
public class QueryPlanBuilder {
  /**
   * Top level method to translate statement to query plan
   *
   * @param stmt statement to be translated
   * @return the root of the query plan
   * @precondition stmt is a Select having a body that is a PlainSelect
   */
  public Operator buildPlan(Statement stmt) {
    // https://github.com/JSQLParser/JSqlParser/wiki/Examples-of-SQL-parsing
    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    Table table = (Table) plainSelect.getFromItem();
    List<Join> joins = plainSelect.getJoins();
    ArrayList<Table> allTables = HelperMethods.getAllTables(table, joins);

    Expression whereExpression = plainSelect.getWhere();
    // start process
    Operator operator;
    if (joins != null) {
      operator = buildJoinPlan(whereExpression, allTables);
    } else {
      operator = new ScanOperator(table);
      if (whereExpression != null) {
        operator = new SelectOperator(operator, whereExpression);
      }
    }

    if (plainSelect.getSelectItems().size() > 1
        || !(plainSelect.getSelectItems().getFirst() instanceof AllColumns)) {
      operator = new ProjectOperator(operator, plainSelect.getSelectItems());
    }

    if (plainSelect.getOrderByElements() != null) {
      operator = new SortOperator(operator, plainSelect.getOrderByElements());
    }

    if (plainSelect.getDistinct() != null) {
      operator = new DuplicateEliminationOperator(operator);
    }

    return operator;
  }

  /**
   * @param whereExpression where expression of the statement what contains join
   * @param allTables all tables in the statement.
   * @return an operator that contained an output schema of joined items.
   */
  private Operator buildJoinPlan(Expression whereExpression, ArrayList<Table> allTables) {
    // NOTE: can only process AND operators. This is enough for Project 1

    // Flatten nested comparisons
    ArrayList<ComparisonOperator> flattened = HelperMethods.flattenExpression(whereExpression);
    Map<String, Expression> tableWhereExpressionMap = new HashMap<>();
    Expression joinWhereExpression = null;
    Expression valueWhereExpression = null;

    /* Separate the comparisons into 3 categories: value comparison, same-table
    comparison, and join comparison */
    for (ComparisonOperator comparisonOperator : flattened) {

      // Get 2 table names from the comparison operator
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
      } else if (leftTableName == null
          || rightTableName == null
          || leftTableName.equals(rightTableName)) {
        // if same-table comparison
        // if one table or both table name are the same, then no join needed.
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

    // SELECT required column from each table individually and put into the queue
    Queue<Operator> operators = new ArrayDeque<>();
    for (Table table : allTables) {
      Operator operator = new ScanOperator(table);
      Alias alias = table.getAlias();
      String name = alias != null ? alias.getName() : table.getName();
      Expression expression = tableWhereExpressionMap.getOrDefault(name, null);
      if (expression != null) {
        // process same-table column comparisons.
        operator = new SelectOperator(operator, expression);
      }
      operators.offer(operator);
    }

    // Concatenate every pairs until 1 item left in queue.
    while (operators.size() > 1) {
      Operator leftChildOperator = operators.poll();
      Operator rightChildOperator = operators.poll();
      Operator operator = new JoinOperator(leftChildOperator, rightChildOperator);
      operators.offer(operator);
    }

    Operator operator = operators.poll();
    /* NOTE: Can move valueComparison to the beginning so we exit before
    // joining the tables. */
    if (valueWhereExpression != null) {
      // if there is a value comparison, should evaluate first.
      operator = new SelectOperator(operator, valueWhereExpression);
    }

    if (joinWhereExpression != null) {
      // if there is a join comparison, should join.
      operator = new SelectOperator(operator, joinWhereExpression);
    }

    return operator;
  }
}
