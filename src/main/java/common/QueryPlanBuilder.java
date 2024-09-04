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
    // All tables in from statement, including join tables
    ArrayList<Table> allTables = HelperMethods.getAllTables(table, joins);

    Expression whereExpression = plainSelect.getWhere();
    // start process
    Operator operator;
    if (joins != null) {
      operator = buildJoinPlan(whereExpression, allTables);
    } else {
      operator = new ScanOperator(table);
      if (whereExpression != null) {
        operator = new SelectOperator(operator.getOutputSchema(), operator, whereExpression);
      }
    }

    if (plainSelect.getSelectItems().size() > 1
        || !(plainSelect.getSelectItems().get(0) instanceof AllColumns)) {
      operator = new ProjectOperator(operator, plainSelect.getSelectItems());
    }

    if (plainSelect.getOrderByElements() != null) {
      operator = new SortOperator(operator.getOutputSchema(), operator, plainSelect.getOrderByElements());
    }

    if (plainSelect.getDistinct() != null) {
      operator = new DuplicateEliminationOperator(operator.getOutputSchema(), operator, plainSelect);
    }

    return operator;
  }

  private Operator buildJoinPlan(Expression whereExpression, ArrayList<Table> allTables) {
    // NOTE: can only process AND operators. This is enough for Project 1
    ArrayList<ComparisonOperator> flattened = HelperMethods.flattenExpression(whereExpression);
    Map<String, Expression> tableWhereExpressionMap = new HashMap<>();
    Expression joinWhereExpression = null;
    Expression valueWhereExpression = null;

    for (ComparisonOperator comparisonOperator : flattened) {
      Pair<String, String> tableNamePair = HelperMethods.getComparisonTableNames(comparisonOperator);
      String leftTableName = tableNamePair.getLeft();
      String rightTableName = tableNamePair.getRight();

      if (leftTableName == null && rightTableName == null) {
        // both are values, ex: 42 = 42, should evaluate first.
        if (valueWhereExpression == null) {
          valueWhereExpression = comparisonOperator;
        } else {
          valueWhereExpression = new AndExpression(valueWhereExpression, comparisonOperator);
        }
      } else if (leftTableName == null || rightTableName == null || leftTableName.equals(rightTableName)) {
        // if one table or both table name are the same, then no join needed.
        String tableName = leftTableName == null ? rightTableName : leftTableName;
        Expression expression = tableWhereExpressionMap.getOrDefault(tableName, null);
        if (expression == null) {
          tableWhereExpressionMap.put(tableName, comparisonOperator);
        } else {
          tableWhereExpressionMap.put(tableName, new AndExpression(expression, comparisonOperator));
        }
      } else {
        // two different tables, should join
        if (joinWhereExpression == null) {
          joinWhereExpression = comparisonOperator;
        } else {
          joinWhereExpression = new AndExpression(joinWhereExpression, comparisonOperator);
        }
      }
    }

    // Scan and select each table individually
    ArrayDeque<Operator> operators = new ArrayDeque<>();
    for (Table table : allTables) {
      Operator operator = new ScanOperator(table);
      Alias alias = table.getAlias();
      String name = alias != null ? alias.getName() : table.getName();
      Expression expression = tableWhereExpressionMap.getOrDefault(name, null);
      if (expression != null) {
        operator = new SelectOperator(operator.getOutputSchema(), operator, expression);
      }
      operators.offer(operator);
    }

    // poll the queue, compute new operator and add back to the queue until 1 item left.
    while (operators.size() > 1) {
      Operator leftChildOperator = operators.poll();
      Operator rightChildOperator = operators.poll();
      Operator operator = new JoinOperator(leftChildOperator, rightChildOperator);
      operators.offer(operator);
    }

    Operator operator = operators.poll();
    // TODO: Can move valueComparison to the beginning so we exit before joining the tables.
    if (valueWhereExpression != null) {
      operator = new SelectOperator(operator.getOutputSchema(), operator, valueWhereExpression);
    }

    if (joinWhereExpression != null) {
      operator = new SelectOperator(operator.getOutputSchema(), operator, joinWhereExpression);
    }

    return operator;
  }
}
