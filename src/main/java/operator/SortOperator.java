package operator;

import common.HelperMethods;
import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * An operator for ORDER BY. Sort the tuples based on the column provided. If there are multiple
 * columns like `ORDER BY a, b, c`, it will preserve the order and only use the subsequent columns
 * to break ties.
 */
public class SortOperator extends Operator {

  private Map<String, Integer> columnIndexMap;
  private List<Tuple> tupleList;
  Iterator<Tuple> it;

  /**
   * SelectOperator constructor
   *
   * @param operator scan | select | join operator
   * @param elementOrders list of ORDER BY elements
   */
  public SortOperator(Operator operator, List<OrderByElement> elementOrders) {
    super(operator.getOutputSchema());

    this.columnIndexMap = HelperMethods.mapColumnIndex(operator.getOutputSchema());

    // sort the tuples
    tupleList = new ArrayList<>(HelperMethods.getAllTuples(operator));
    Collections.sort(
        tupleList,
        new Comparator<Tuple>() {
          @Override
          public int compare(Tuple t1, Tuple t2) {
            for (OrderByElement elementOrder : elementOrders) {
              Column column = (Column) elementOrder.getExpression();
              int index = columnIndexMap.get(column.getName(true));
              int compare =
                  Integer.compare(t1.getElementAtIndex(index), t2.getElementAtIndex(index));
              if (compare != 0) {
                return compare;
              }
            }
            return 0;
          }
        });
    it = tupleList.iterator();
  }

  /** Re-initialize iterator */
  @Override
  public void reset() {
    it = tupleList.iterator();
  }

  /**
   * @return individual tuples from the child operator's all tuples
   */
  @Override
  public Tuple getNextTuple() {
    if (it.hasNext()) {
      return it.next();
    }
    return null;
  }
}
