package physical_operator;

import common.HelperMethods;
import common.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;

/** Sort Merge Join Operator */
public class SMJOperator extends Operator {

  private Operator leftChildOperator;
  private Operator rightChildOperator;
  private int leftColumnIndex;
  private int rightColumnIndex;

  private Tuple leftTuple;
  private int rightCurrentIndex;
  private Map<Integer, Integer> rightResetIndexMap;
  private boolean reverse;

  /**
   * SMJOperator Constructor
   *
   * @param outputSchema output schema
   * @param leftChildOperator leftChildOperator that needs to perform to join
   * @param rightChildOperator rightChildOperator that needs to perform to join
   * @param leftColumn left column to join
   * @param rightColumn right column to join
   */
  public SMJOperator(
      ArrayList<Column> outputSchema,
      Operator leftChildOperator,
      Operator rightChildOperator,
      Column leftColumn,
      Column rightColumn,
      boolean reverse) {
    super(outputSchema);
    this.leftChildOperator = leftChildOperator;
    this.rightChildOperator = rightChildOperator;
    this.reverse = reverse;

    Map<String, Integer> leftColumnMap =
        HelperMethods.mapColumnIndex(leftChildOperator.getOutputSchema());
    Map<String, Integer> rightColumnMap =
        HelperMethods.mapColumnIndex(rightChildOperator.getOutputSchema());

    leftColumnIndex = leftColumnMap.get(leftColumn.getName(true));
    rightColumnIndex = rightColumnMap.get(rightColumn.getName(true));

    leftTuple = leftChildOperator.getNextTuple();
    rightCurrentIndex = -1;
    rightResetIndexMap = new HashMap<>();
  }

  @Override
  public void reset() {
    leftChildOperator.reset();
    rightChildOperator.reset();
    leftTuple = leftChildOperator.getNextTuple();
    rightCurrentIndex = -1;
    rightResetIndexMap.clear();
  }

  @Override
  public Tuple getNextTuple() {
    // merge sorted tuples
    Tuple rightTuple = rightChildOperator.getNextTuple();
    rightCurrentIndex++;

    while (leftTuple != null && rightTuple != null) {
      int leftVal = leftTuple.getElementAtIndex(leftColumnIndex);
      int rightVal = rightTuple.getElementAtIndex(rightColumnIndex);

      if (leftVal == rightVal) {
        if (!rightResetIndexMap.containsKey(rightVal)) {
          // value to index map
          rightResetIndexMap.put(rightVal, rightCurrentIndex);
        }
        return this.reverse ? rightTuple.concat(leftTuple) : leftTuple.concat(rightTuple);
      }

      if (leftVal < rightVal) {
        // if left value is smaller, move left
        leftTuple = leftChildOperator.getNextTuple();
        if (leftTuple == null) {
          return null;
        }
        if (checkAndResetIndex()) {
          rightTuple = rightChildOperator.getNextTuple();
          return this.reverse ? rightTuple.concat(leftTuple) : leftTuple.concat(rightTuple);
        }
      } else {
        // if right value is smaller, move right
        rightTuple = rightChildOperator.getNextTuple();
        rightCurrentIndex++;
      }
    }

    if (leftTuple == null) {
      return null;
    }

    if (rightTuple == null) {
      leftTuple = leftChildOperator.getNextTuple();
      if (leftTuple == null) {
        return null;
      }
      if (checkAndResetIndex()) {
        rightTuple = rightChildOperator.getNextTuple();
        return this.reverse ? rightTuple.concat(leftTuple) : leftTuple.concat(rightTuple);
      }
    }
    return null;
  }

  /**
   * Check if the index needs to be reset and reset the index
   *
   * @return
   */
  private boolean checkAndResetIndex() {
    int value = leftTuple.getElementAtIndex(leftColumnIndex);
    if (!rightResetIndexMap.containsKey(value)) {
      return false;
    }
    int index = rightResetIndexMap.get(value);
    if (rightChildOperator instanceof SortOperator) {
      ((SortOperator) rightChildOperator).reset(index);
    } else if (rightChildOperator instanceof ExternalSortOperator) {
      ((ExternalSortOperator) rightChildOperator).reset(index);
    }
    rightCurrentIndex = index - 1;
    return true;
  }
}
