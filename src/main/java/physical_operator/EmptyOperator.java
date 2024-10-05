package physical_operator;

import common.Tuple;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;

public class EmptyOperator extends Operator {

  public EmptyOperator() {
    super(null);
  }

  /**
   *
   */
  @Override
  public void reset() {

  }

  /**
   * @return
   */
  @Override
  public Tuple getNextTuple() {
    return null;
  }
}
