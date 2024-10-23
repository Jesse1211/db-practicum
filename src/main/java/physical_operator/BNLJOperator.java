package physical_operator;

import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/**
 * Block Nested Loop Join Operator
 */
public class BNLJOperator extends Operator {

  private Tuple[] tupleBuffer;
  private int leftTupleBlockIndex;
  private int bufferSizeInPage;
  private Operator leftChildOperator;
  private Operator rightChildOperator;
  private Tuple rightTuple;

  /**
   * Block Nested Loop Join Operator Constructor
   * 
   * @param outputSchema       output schema
   * @param leftChildOperator  left child operator
   * @param rightChildOperator right child operator
   * @param bufferSizeInPage   buffer size in page unit, each page is 4096 bytes,
   *                           buffer means the block size
   */
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

  /**
   * Load left child block into memory
   * 
   * @return true if there is at least one tuple in the block, false otherwise
   */
  private boolean loadLeftChildBlock() {
    // Pages per block * page size / integer size / tuple size
    int maxTupleNum = bufferSizeInPage * 4096 / 4 / leftChildOperator.getOutputSchema().size();
    tupleBuffer = new Tuple[maxTupleNum];
    this.leftTupleBlockIndex = 0;
    return loadTupleBlock(leftChildOperator, tupleBuffer);
  }

  /**
   * Output one tuple at a time, block buffer can be resumed. Traverse ALL right
   * tuples for each
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
