package operator;

import common.ExpressionEvaluator;
import common.HelperMethods;
import common.Tuple;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {

  private Operator childOperator;
  private Expression whereExpression;
  private Map<String, Integer> columnIndexMap;

  /**
   * Process comparators, Filter rows
   * @param childOperator scan operator
   * @param whereExpression WHERE expressions as 'Table.column = value' expression
   */
  public SelectOperator(Operator childOperator, Expression whereExpression) {
    super(childOperator.getOutputSchema());
    this.childOperator = childOperator;
    this.whereExpression = whereExpression;
    this.columnIndexMap = HelperMethods.mapColumnIndex(outputSchema);
  }

  /**
   * Invoke childOperator's reset method
   */
  @Override
  public void reset() {
    childOperator.reset();
  }

  /**
   * Return satisfied row as tuple based on `ExpressionEvaluator`
   */
  @Override
  public Tuple getNextTuple() {
    Tuple tuple;

    if ((tuple = childOperator.getNextTuple()) != null) {
      // Use ExpressionEvaluator to evaluate tuple, if the condition matches, return
      // the tuple。 Otherwise, continue checking the next tuple.
      ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, columnIndexMap);
      whereExpression.accept(evaluator);
      boolean result = evaluator.getResult();
      if (result) {
        return tuple;
      } else {
        return getNextTuple();
      }
    }
    return null;
  }
}
