package operator;

import common.Tuple;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;

// Build a query plan that is a tree of operators.
public class SelectOperator extends Operator {

  // private final List<Operator> child;

  public SelectOperator(ArrayList<Column> outputSchema) {
    super(outputSchema);
  }

  @Override
  public void reset() {
    // for (Operator operator : child) {
    // operator.reset();
    // }
  }

  @Override
  public Tuple getNextTuple() {
    // todo: 这里就是计算每一个operator的getNextTuple()的结果 = 具体的evaluate
    // WHERE query: SELECT * FROM table WHERE column = value => 有可能需要child来计算
    Tuple tuple = null;
    // for (Operator operator : child) {
    // tuple = operator.getNextTuple();
    // if (tuple != null) {
    // break;
    // }
    // }
    return tuple;
  }
}
