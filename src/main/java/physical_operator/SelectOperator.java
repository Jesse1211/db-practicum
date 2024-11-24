package physical_operator;

import compiler.ExpressionEvaluator;
import common.HelperMethods;
import common.tuple.Tuple;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/**
 * An operator for WHERE. Only return the tuples that matches the comparison expression (only when
 * the expression evaluates to true).
 */
public class SelectOperator extends Operator {

  private Operator childOperator;
  private Expression whereExpression;
  private Map<String, Integer> columnIndexMap;

  /**
   * SelectOperator Constructor
   *
   * @param childOperator scan operator
   * @param whereExpression WHERE expressions as 'Table.column = value' expression
   */
  public SelectOperator(
      ArrayList<Column> outputSchema, Operator childOperator, Expression whereExpression) {
    super(outputSchema);
    this.childOperator = childOperator;
    this.whereExpression = whereExpression;
    this.columnIndexMap = HelperMethods.mapColumnIndex(outputSchema);
  }

  /** Invoke childOperator's reset method */
  @Override
  public void reset() {
    childOperator.reset();
  }

  /**
   * @return satisfied row as tuple based on `ExpressionEvaluator`
   */
  @Override
  public Tuple getNextTuple() {
    Tuple tuple;

    while ((tuple = childOperator.getNextTuple()) != null) {
      ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, columnIndexMap);
      whereExpression.accept(evaluator);
      boolean result = evaluator.getResult();
      if (result) {
        return tuple;
      }
    }
    return null;
  }
}
