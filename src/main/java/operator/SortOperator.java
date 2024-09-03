package operator;

import common.HelperMethods;
import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/*
 * we only want to sort in ascending order
 * the attributes mentioned in the ORDER BY are a subset of those retained by the SELECT. This allows you to do the sorting last, after projection. 
 * Note that this does not mean that every attribute in ORDER BY must be mentioned in the SELECT - a query like SELECT * FROM Sailors S ORDER BY S.name is valid.
 */
public class SortOperator extends Operator {

  private Map<String, Integer> columnIndexMap;
  private List<Tuple> tupleList;
  Iterator<Tuple> it;

  public SortOperator(ArrayList<Column> outputSchema, Operator operator, List<OrderByElement> elementOrders) {
    super(outputSchema);

    this.columnIndexMap = HelperMethods.mapColumnIndex(operator.getOutputSchema());

    // sort the tuples
    tupleList = new ArrayList<>(HelperMethods.collectAllTuples(operator));
    Collections.sort(tupleList, new Comparator<Tuple>() {
      @Override
      public int compare(Tuple t1, Tuple t2) {
        for (OrderByElement elementOrder : elementOrders) {
          Column column = (Column) elementOrder.getExpression();
          int index = columnIndexMap.get(column.getName(false));
          int compare = Integer.compare(t1.getElementAtIndex(index), t2.getElementAtIndex(index));
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    });
    it = tupleList.iterator();
  }

  @Override
  public void reset() {
    it = tupleList.iterator();
  }

  /*
   * read all of the output from its child, place it into an internal buffer, sort
   * it, and then return individual tuples when requested.
   */
  @Override
  public Tuple getNextTuple() {
    if (it.hasNext()) {
      return it.next();
    }
    return null;
  }
}
