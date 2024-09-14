package common;

import java.util.Map;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/** Expression evaluator that evaluates the comparison and return a boolean result. */
public class ExpressionEvaluator extends ExpressionVisitorAdapter {
  private Tuple tuple;
  private Map<String, Integer> columnIndexMap;
  private boolean result;
  private int value;

  public ExpressionEvaluator(Tuple tuple, Map<String, Integer> columnIndexMap) {
    this.tuple = tuple;
    this.columnIndexMap = columnIndexMap;
  }

  public boolean getResult() {
    return result;
  }

  @Override
  public void visit(LongValue longValue) {
    value = (int) longValue.getValue();
  }

  @Override
  public void visit(Column column) {
    int index = columnIndexMap.get(column.getName(true));
    value = tuple.getElementAtIndex(index);
  }

  @Override
  public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    boolean leftResult = getResult();
    andExpression.getRightExpression().accept(this);
    boolean rightResult = getResult();

    result = leftResult && rightResult;
  }

  @Override
  public void visit(OrExpression orExpression) {
    orExpression.getLeftExpression().accept(this);
    boolean leftResult = getResult();

    orExpression.getRightExpression().accept(this);
    boolean rightResult = getResult();

    result = leftResult || rightResult;
  }

  @Override
  public void visit(XorExpression xorExpression) {
    xorExpression.getLeftExpression().accept(this);
    boolean leftResult = getResult();

    xorExpression.getRightExpression().accept(this);
    boolean rightResult = getResult();

    result = leftResult ^ rightResult;
  }

  @Override
  public void visit(EqualsTo equalsTo) {
    equalsTo.getLeftExpression().accept(this);
    int leftValue = value;
    equalsTo.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue == rightValue;
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    greaterThan.getLeftExpression().accept(this);
    int leftValue = value;
    greaterThan.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue > rightValue;
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    greaterThanEquals.getLeftExpression().accept(this);
    int leftValue = value;
    greaterThanEquals.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue >= rightValue;
  }

  @Override
  public void visit(MinorThan minorThan) {
    minorThan.getLeftExpression().accept(this);
    int leftValue = value;
    minorThan.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue < rightValue;
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    minorThanEquals.getLeftExpression().accept(this);
    int leftValue = value;
    minorThanEquals.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue <= rightValue;
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    notEqualsTo.getLeftExpression().accept(this);
    int leftValue = value;
    notEqualsTo.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue != rightValue;
  }
}
