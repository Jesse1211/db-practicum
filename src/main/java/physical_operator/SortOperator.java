package physical_operator;

import common.HelperMethods;
import common.Tuple;
import java.util.*;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * An operator for ORDER BY. Sort the tuples based on the column provided. If there are multiple
 * columns like `ORDER BY a, b, c`, it will preserve the order and only use the subsequent columns
 * to break ties.
 */
public class SortOperator extends Operator {

  Iterator<Tuple> it;
  private List<Tuple> tupleList;
  private List<Column> orders;

  /**
   * SelectOperator constructor
   *
   * @param childOperator scan | select | join operator
   * @param orders list of columns in ORDER BY elements
   */
  public SortOperator(
      ArrayList<Column> outputSchema, Operator childOperator, List<Column> orders) {
    super(outputSchema);
    this.orders = orders;
    // sort the tuples
    this.tupleList = new ArrayList<>(HelperMethods.getAllTuples(childOperator));
    sort();
    this.it = tupleList.iterator();
  }

  public SortOperator(ArrayList<Column> outputSchema, Operator operator, Column order) {
    super(outputSchema);
    this.orders = new ArrayList<>();
    this.orders.add(order);
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
    Collections.sort(tupleList, HelperMethods.getTupleComparator(orders, outputSchema));
  }

  /** Re-initialize iterator */
  @Override
  public void reset() {
    it = tupleList.listIterator(0);
  }

  @Override
  public void reset(int i) {
    it = tupleList.listIterator(i);
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