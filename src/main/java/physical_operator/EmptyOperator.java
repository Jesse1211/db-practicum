package physical_operator;

import common.tuple.Tuple;

/**
 * An operator for EMPTY, used to represent an empty operator. It is used to represent the end of
 * the operator chain.
 */
public class EmptyOperator extends Operator {

  public EmptyOperator() {
    super(null);
  }

  @Override
  public void reset() {}

  @Override
  public Tuple getNextTuple() {
    return null;
  }
}
