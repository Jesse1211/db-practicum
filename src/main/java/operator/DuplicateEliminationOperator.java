package operator;

import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

/*
 * [Assumes the input from its child is in sorted order]
 * Reads the tuples from the child and output non-duplicate tuples.
 */
public class DuplicateEliminationOperator extends Operator {
  private Operator operator;

  // key: column index, value: value in that row of that column
  private Set<Tuple> distinctSet = new HashSet<>();

  public DuplicateEliminationOperator(
      ArrayList<Column> outputSchema, Operator operator, PlainSelect plainSelect) {
    super(outputSchema);
    this.operator = operator;
  }

  @Override
  public void reset() {
    operator.reset();
    distinctSet.clear();
  }

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
