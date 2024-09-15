package operator;

import common.Tuple;
import java.util.*;

/**
 * An operator for DISTINCT, used to eliminate duplicate tuples. It uses a set to track visited
 * tuples, and if a tuple is already in the set, skip the one.
 */
public class DuplicateEliminationOperator extends Operator {
  private Operator operator;
  private Set<Tuple> distinctSet = new HashSet<>();

  /**
   * DuplicateEliminationOperator constructor
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
