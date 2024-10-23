package physical_operator;

import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

public class BNLJOperator extends Operator {

  private Tuple[] leftTupleBlock;
  private int leftTupleBlockIndex;
  private int bufferSizeInPage;
  private Operator leftChildOperator;
  private Operator rightChildOperator;
  private Tuple rightTuple;

  public BNLJOperator(
      ArrayList<Column> outputSchema,
      Operator leftChildOperator,
      Operator rightChildOperator,
      int bufferSizeInPage) {

    super(outputSchema);
    this.bufferSizeInPage = bufferSizeInPage;
    this.leftChildOperator = leftChildOperator;
    this.rightChildOperator = rightChildOperator;

    // Load for the first time
    this.rightTuple = rightChildOperator.getNextTuple();
    loadLeftChildBlock();
  }

  private boolean loadLeftChildBlock() {
    Tuple tuple = leftChildOperator.getNextTuple();

    if (tuple == null) {
      return false;
    }

    int maxTupleNum = bufferSizeInPage * 4096 / 4 / tuple.getSize();
    leftTupleBlock = new Tuple[maxTupleNum];
    leftTupleBlock[0] = tuple;
    int index = 1;

    while (index < maxTupleNum) {
      tuple = leftChildOperator.getNextTuple();
      if (tuple == null) {
        break;
      }
      leftTupleBlock[index++] = tuple;
    }
    this.leftTupleBlockIndex = 0;
    return true;
  }

  /**
   * Output one tuple at a time, block buffer can be resumed. Traverse ALL right tuples for each
   * left tuple in the block.
   *
   * @return a tuple that glues left and right tuples
   */
  @Override
  public Tuple getNextTuple() {

    // Traversed ONE left tuple block, reset index & load next right tuple
    if (this.leftTupleBlockIndex >= this.tupleBuffer.length
        || this.tupleBuffer[this.leftTupleBlockIndex] == null) {
      this.leftTupleBlockIndex = 0;
      this.rightTuple = rightChildOperator.getNextTuple();
    }

    // No more right tuple, reset right tuple & load next left tuple block
    if (this.rightTuple == null) {
      if (!loadLeftChildBlock()) {
        return null;
      }
      this.rightChildOperator.reset();
      this.rightTuple = rightChildOperator.getNextTuple();
    }

    if (this.tupleBuffer == null) {
      return null;
    }

    return this.tupleBuffer[this.leftTupleBlockIndex++].concat(rightTuple);
  }

  @Override
  public void reset() {
    leftChildOperator.reset();
    rightChildOperator.reset();
  }
}
