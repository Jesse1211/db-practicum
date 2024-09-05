package operator;

import common.Tuple;
import java.util.*;

public class DuplicateEliminationOperator extends Operator {
  private Operator operator;
  private Set<Tuple> distinctSet = new HashSet<>();

  /**
   * Avoid duplicate tuples in output
   *
   * @param operator child operator, this is invoked as last operator in plan builder
   */
  public DuplicateEliminationOperator(Operator operator) {
    super(operator.getOutputSchema());
    this.operator = operator;
  }

  /** Clear all visited tuples */
  @Override
  public void reset() {
    operator.reset();
    distinctSet.clear();
  }

  /** Return only distinct tuples */
  @Override
  public Tuple getNextTuple() {
    Tuple tuple;

    if ((tuple = operator.getNextTuple()) != null) {
      if (!distinctSet.contains(tuple)) {
        distinctSet.add(tuple);
        return tuple;
      } else {
        return getNextTuple();
      }
    }
    return null;
  }
}
