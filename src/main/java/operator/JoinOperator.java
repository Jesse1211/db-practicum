package operator;

import common.ExpressionEvaluator;
import common.HelperMethods;
import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/*
 * Operator for the Project operation: SELECT *Sailors.id* FROM Sailors WHERE Sailors.age = 20
 * [Assume that there are still no table aliases, so you don’t have to worry about self-joins for now.]
 */
public class JoinOperator extends Operator {
  private Operator left;
  private Operator right;

  private Map<String, Integer> columnMap = new HashMap<>(); // column name : index
  private Expression joinExpression;
  private Tuple leftTuple = null;

  public JoinOperator(
      ArrayList<Column> outputSchema, Operator left, Operator right, Expression joinExpression) {
    super(outputSchema);

    this.left = left;
    this.right = right;

    outputSchema = new ArrayList<>();
    outputSchema.addAll(left.getOutputSchema());
    outputSchema.addAll(right.getOutputSchema());

    columnMap = HelperMethods.mapColumnIndex(outputSchema);

    this.joinExpression = joinExpression;
  }

  /** Reset the operator to the start. */
  @Override
  public void reset() {
    left.reset();
    right.reset();
  }

  @Override
  public Tuple getNextTuple() {
    Tuple rightTuple = right.getNextTuple();
    // 左边走一行 - 右边走一遍
    if (rightTuple == null) {
      right.reset();
      leftTuple = left.getNextTuple();
      rightTuple = right.getNextTuple();
    }

    if (leftTuple == null || rightTuple == null) {
      return null;
    }

    // Glues them together
    Tuple joinedTuple = joinTuples(leftTuple, rightTuple);

    ExpressionEvaluator evaluator = new ExpressionEvaluator(joinedTuple, columnMap);
    joinExpression.accept(evaluator);
    boolean result = evaluator.getResult();
    if (result) {
      return joinedTuple;
    } else {
      getNextTuple();
    }

    return joinedTuple;
  }

  private Tuple joinTuples(Tuple leftTuple, Tuple rightTuple) {
    ArrayList<Integer> tupleArray = new ArrayList<>();

    tupleArray.addAll(leftTuple.getAllElements());
    tupleArray.addAll(rightTuple.getAllElements());

    return new Tuple(tupleArray);
  }
}
