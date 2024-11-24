package physical_operator;

import common.tuple.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/*
 * Operator for the Project operation: SELECT *Sailors.id* FROM Sailors WHERE Sailors.age = 20
 * [Assume that there are still no table aliases, so you don’t have to worry about self-joins for now.]
 */
public class JoinOperator extends Operator {

  private Operator leftChildOperator;
  private Operator rightChildOperator;

  private Tuple leftTuple;

  /**
   * JoinOperator Constructor
   *
   * @param leftChildOperator leftChildOperator that needs to perform to join
   * @param rightChildOperator rightChildOperator that needs to perform to join
   */
  public JoinOperator(
      ArrayList<Column> outputSchema, Operator leftChildOperator, Operator rightChildOperator) {
    super(outputSchema);

    this.leftChildOperator = leftChildOperator;
    this.rightChildOperator = rightChildOperator;

    leftTuple = leftChildOperator.getNextTuple();
  }

  @Override
  public void reset() {
    leftChildOperator.reset();
    rightChildOperator.reset();
    leftTuple = leftChildOperator.getNextTuple();
  }

  /**
   * for each left tuple, concatenate with each of the right tuples.
   *
   * @return a tuple that glues left and right tuples
   */
  @Override
  public Tuple getNextTuple() {
    Tuple rightTuple = rightChildOperator.getNextTuple();

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
