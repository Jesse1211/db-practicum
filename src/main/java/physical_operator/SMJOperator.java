package physical_operator;

import common.HelperMethods;
import common.Tuple;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;

public class SMJOperator extends Operator {

  private SortOperator leftChildOperator;
  private SortOperator rightChildOperator;
  private int leftColumnIndex;
  private int rightColumnIndex;

  private Tuple leftTuple;
  private int rightCurrentIndex;
  private Map<Integer, Integer> rightResetIndexMap;

  /**
   * SMJOperator Constructor
   *
   * @param leftChildOperator  leftChildOperator that needs to perform to join
   * @param rightChildOperator rightChildOperator that needs to perform to join
   */
  public SMJOperator(
          ArrayList<Column> outputSchema,
          SortOperator leftChildOperator,
          SortOperator rightChildOperator,
          Column leftColumn,
          Column rightColumn
  ) {
    super(outputSchema);
    this.leftChildOperator = leftChildOperator;
    this.rightChildOperator = rightChildOperator;

    Map<String, Integer> leftColumnMap = HelperMethods.mapColumnIndex(
            leftChildOperator.getOutputSchema());
    Map<String, Integer> rightColumnMap = HelperMethods.mapColumnIndex(
            rightChildOperator.getOutputSchema());

    leftColumnIndex = leftColumnMap.get(leftColumn.getName(true));
    rightColumnIndex = rightColumnMap.get(rightColumn.getName(true));

    leftTuple = leftChildOperator.getNextTuple();
    rightCurrentIndex = -1;
  }


  /**
   *
   */
  @Override
  public void reset() {
    leftChildOperator.reset();
    rightChildOperator.reset();
    leftTuple = leftChildOperator.getNextTuple();
    rightCurrentIndex = -1;
    rightResetIndexMap.clear();
  }


  /**
   * @return
   */
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
        return leftTuple.concat(rightTuple);
      }

      if (leftVal < rightVal) {
        // if left value is smaller, move left
        leftTuple = leftChildOperator.getNextTuple();
        if (leftTuple == null) {
          return null;
        }
        if (checkAndResetIndex()) {
          rightTuple = rightChildOperator.getNextTuple();
          return leftTuple.concat(rightTuple);
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
        return leftTuple.concat(rightTuple);
      }
    }
    return null;
  }

  private boolean checkAndResetIndex() {
    int value = leftTuple.getElementAtIndex(leftColumnIndex);
    if (!rightResetIndexMap.containsKey(value)) {
      return false;
    }
    int index = rightResetIndexMap.get(value);
    rightChildOperator.reset(index);
    rightCurrentIndex = index - 1;
    return true;
  }

}
