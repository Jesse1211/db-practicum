package physical_operator;

import common.ExpressionEvaluator;
import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/**
 * An operator for DISTINCT, used to eliminate duplicate tuples. It uses a set to track visited
 * tuples, and if a tuple is already in the set, skip the one.
 */
public class DuplicateEliminationOperator extends Operator {
  private Operator childOperator;
  private Tuple prevTuple = null;

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
  }

  @Override
  public Tuple getNextTuple() {
    Tuple tuple;

    while ((tuple = childOperator.getNextTuple()) != null && tuple.equals(prevTuple));
    prevTuple = tuple;
    return tuple;
  }
}
