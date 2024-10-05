package physical_operator;

import common.Tuple;

public class EmptyOperator extends Operator {

  public EmptyOperator() {
    super(null);
  }

  /** */
  @Override
  public void reset() {}

  /**
   * @return
   */
  @Override
  public Tuple getNextTuple() {
    return null;
  }
}
