package operator;

import common.Tuple;
import java.util.*;

/*
 * Operator for the Project operation: SELECT *Sailors.id* FROM Sailors WHERE Sailors.age = 20
 * [Assume that there are still no table aliases, so you don’t have to worry about self-joins for now.]
 */
public class JoinOperator extends Operator {

  private Operator leftChildOperator;
  private Operator rightChildOperator;

  private Tuple leftTuple;

  public JoinOperator(Operator leftChildOperator, Operator rightChildOperator) {
    super(new ArrayList<>());

    this.leftChildOperator = leftChildOperator;
    this.rightChildOperator = rightChildOperator;

    this.outputSchema.addAll(leftChildOperator.getOutputSchema());
    this.outputSchema.addAll(rightChildOperator.getOutputSchema());

    leftTuple = leftChildOperator.getNextTuple();
  }

  /** Reset the operator to the start. */
  @Override
  public void reset() {
    leftChildOperator.reset();
    rightChildOperator.reset();
    leftTuple = leftChildOperator.getNextTuple();
  }

  @Override
  public Tuple getNextTuple() {
    Tuple rightTuple = rightChildOperator.getNextTuple();
    // 左边走一行 - 右边走一遍
    if (rightTuple == null) {
      rightChildOperator.reset();
      leftTuple = leftChildOperator.getNextTuple();
      rightTuple = rightChildOperator.getNextTuple();
    }

    if (leftTuple == null || rightTuple == null) {
      return null;
    }

    // Glues them together
    return leftTuple.concat(rightTuple);
  }
}
