package physical_operator;

import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/**
 * An operator for DISTINCT, used to eliminate duplicate tuples. It uses a set to track visited
 * tuples, and if a tuple is already in the set, skip the one.
 */
public class DuplicateEliminationOperator extends Operator {
  private Operator childOperator;
  private Set<Tuple> distinctSet = new HashSet<>();

  /**
   * DuplicateEliminationOperator constructor
   *
   * @param childOperator child operator, this is invoked as last operator in plan builder
   */
  public DuplicateEliminationOperator(ArrayList<Column> outputSchema, Operator childOperator) {
    super(outputSchema);
    this.childOperator = childOperator;
  }

  @Override
  public void reset() {
    childOperator.reset();
    distinctSet.clear();
  }

  @Override
  public Tuple getNextTuple() {
    Tuple tuple;

    if ((tuple = childOperator.getNextTuple()) != null) {
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
