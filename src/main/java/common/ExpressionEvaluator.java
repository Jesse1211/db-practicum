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
  private int value;  /**
   * constructor of ExpressionEvaluator
   *
   * @param tuple current tuple
   * @param columnIndexMap columnIndexMap that use column names for keys and column index for values
   */
  public ExpressionEvaluator(Tuple tuple, Map<String, Integer> columnIndexMap) {
    this.tuple = tuple;
    this.columnIndexMap = columnIndexMap;
  }

  public ExpressionEvaluator() {
  }

  /**
   * get the result from this expression evaluator
   *
   * @return the result in a boolean
   */
  public boolean getResult() {
    return result;
  }

  /**
   * if the expression is a value, set the value and no action is needed.
   *
   * @param longValue
   */
  @Override
  public void visit(LongValue longValue) {
    value = (int) longValue.getValue();
  }

  /**
   * if the expression is a column, get the corresponding value from the current tuple
   *
   * @param column
   */
  @Override
  public void visit(Column column) {
    int index = columnIndexMap.get(column.getName(true));
    value = tuple.getElementAtIndex(index);
  }

  /**
   * evaluate AndExpression, the recursively evaluate nested expressions
   *
   * @param andExpression
   */
  @Override
  public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    boolean leftResult = getResult();
    andExpression.getRightExpression().accept(this);
    boolean rightResult = getResult();

    result = leftResult && rightResult;
  }

  /**
   * Not used in this project because of precondition. evaluate OrExpression, the recursively
   * evaluate nested expressions
   *
   * @param orExpression
   */
  @Override
  public void visit(OrExpression orExpression) {
    orExpression.getLeftExpression().accept(this);
    boolean leftResult = getResult();

    orExpression.getRightExpression().accept(this);
    boolean rightResult = getResult();

    result = leftResult || rightResult;
  }

  /**
   * Not used in this project because of precondition. evaluate xorExpression, the recursively
   * evaluate nested expressions
   *
   * @param xorExpression
   */
  @Override
  public void visit(XorExpression xorExpression) {
    xorExpression.getLeftExpression().accept(this);
    boolean leftResult = getResult();

    xorExpression.getRightExpression().accept(this);
    boolean rightResult = getResult();

    result = leftResult ^ rightResult;
  }

  /**
   * evaluate equalsTo, set the result as leftValue == rightValue
   *
   * @param equalsTo
   */
  @Override
  public void visit(EqualsTo equalsTo) {
    equalsTo.getLeftExpression().accept(this);
    int leftValue = value;
    equalsTo.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue == rightValue;
  }

  /**
   * evaluate greaterThan, set the result as leftValue > rightValue
   *
   * @param greaterThan
   */
  @Override
  public void visit(GreaterThan greaterThan) {
    greaterThan.getLeftExpression().accept(this);
    int leftValue = value;
    greaterThan.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue > rightValue;
  }

  /**
   * evaluate greaterThanEquals, set the result as leftValue >= rightValue
   *
   * @param greaterThanEquals
   */
  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    greaterThanEquals.getLeftExpression().accept(this);
    int leftValue = value;
    greaterThanEquals.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue >= rightValue;
  }

  /**
   * evaluate minorThan, set the result as leftValue < rightValue
   *
   * @param minorThan
   */
  @Override
  public void visit(MinorThan minorThan) {
    minorThan.getLeftExpression().accept(this);
    int leftValue = value;
    minorThan.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue < rightValue;
  }

  /**
   * evaluate minorThanEquals, set the result as leftValue <= rightValue
   *
   * @param minorThanEquals
   */
  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    minorThanEquals.getLeftExpression().accept(this);
    int leftValue = value;
    minorThanEquals.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue <= rightValue;
  }

  /**
   * evaluate notEqualsTo, set the result as leftValue <>/!= rightValue
   *
   * @param notEqualsTo
   */
  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    notEqualsTo.getLeftExpression().accept(this);
    int leftValue = value;
    notEqualsTo.getRightExpression().accept(this);
    int rightValue = value;

    result = leftValue != rightValue;
  }
}
