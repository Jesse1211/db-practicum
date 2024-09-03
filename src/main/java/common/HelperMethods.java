package common;

import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import operator.Operator;

public class HelperMethods {
  public static Map<String, Integer> mapColumnIndex(ArrayList<Column> columns) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (int i = 0; i < columns.size(); i++) {
      map.put(columns.get(i).getName(false), i);
    }
    return map;
  }

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

  public static Map<String, String> updateAliasInTable(PlainSelect plainSelect) {
    Map<String, String> tableAliasToName = new HashMap<String, String>(); // alias to name

    Table table = (Table) plainSelect.getFromItem();
    tableAliasToName.put(table.getName(), table.getName());
    if (table.getAlias() != null) {
      tableAliasToName.put(table.getAlias().getName(), table.getName());
    }

    // 可能有多个table & alias
    if (plainSelect.getJoins() != null) {
      for (Join join : plainSelect.getJoins()) {
        var curTable = (Table) join.getRightItem();
        tableAliasToName.put(curTable.getAlias().getName(), curTable.getName());
      }
    }
    return tableAliasToName;
  }

  public static List<Tuple> collectAllTuples(Operator operator) {
    Tuple tuple;
    List<Tuple> tuples = new ArrayList<>();
    while ((tuple = operator.getNextTuple()) != null) {
      tuples.add(tuple);
    }

    return tuples;
  }
}
