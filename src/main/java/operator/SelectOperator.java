package operator;

import common.ExpressionEvaluator;
import common.HelperMethods;
import common.Tuple;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

// Build a query plan that is a tree of operators.
public class SelectOperator extends Operator {

  private Operator childOperator;
  private Expression whereExpression;
  private Map<String, Integer> columnIndexMap;

  public SelectOperator(
      ArrayList<Column> outputSchema, Operator childOperator, Expression whereExpression) {
    super(outputSchema);
    this.childOperator = childOperator;
    this.whereExpression = whereExpression;
    this.columnIndexMap = HelperMethods.mapColumnIndex(outputSchema);
  }

  @Override
  public void reset() {
    childOperator.reset();
  }

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
