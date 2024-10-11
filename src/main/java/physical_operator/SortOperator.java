package physical_operator;

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

  Iterator<Tuple> it;
  private Map<String, Integer> columnIndexMap;
  private List<Tuple> tupleList;
  private List<OrderByElement> elementOrders;

  /**
   * SelectOperator constructor
   *
   * @param operator scan | select | join operator
   * @param elementOrders list of ORDER BY elements
   */
  public SortOperator(
      ArrayList<Column> outputSchema, Operator operator, List<OrderByElement> elementOrders) {
    super(outputSchema);
    this.columnIndexMap = HelperMethods.mapColumnIndex(operator.getOutputSchema());
    this.elementOrders = elementOrders;
    // sort the tuples
    this.tupleList = new ArrayList<>(HelperMethods.getAllTuples(operator));
    sort();
    this.it = tupleList.iterator();
  }

  /**
   * Sort the tuples based on the column specified in the ORDER BY clause. Then sort the tuples
   * based on the subsequent columns to break ties.
   */
  private void sort() {
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

              // if the attributes are not equal, return the comparison result
              if (compare != 0) {
                return compare;
              }
            }

            // if the attributes are equal, traverse columnIndexMap to compare the next
            // non-equal column
            for (Column column : getOutputSchema()) {
              String key = column.getName(true);
              if (t1.getElementAtIndex(columnIndexMap.get(key))
                  != t2.getElementAtIndex(columnIndexMap.get(key))) {
                return Integer.compare(
                    t1.getElementAtIndex(columnIndexMap.get(key)),
                    t2.getElementAtIndex(columnIndexMap.get(key)));
              }
            }
            return 0;
          }
        });
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
