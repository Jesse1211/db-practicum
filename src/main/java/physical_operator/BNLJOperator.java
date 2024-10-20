package physical_operator;

import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/**
 * Logic: procesdures BNLJ(outer R, inner S, bufferSize B): for each block B of R do: for each tuple
 * s of S do: for each tuple r of B do: if r and s join then: add r and s as tuple to the result
 */
public class BNLJOperator extends Operator {

  private List<Tuple> leftTupleBlock;
  private int leftTupleBlockIndex;
  private int maxSlotNum;
  private Operator leftChildOperator;
  private Operator rightChildOperator;
  private Tuple rightTuple;

  public BNLJOperator(
      ArrayList<Column> outputSchema,
      Operator leftChildOperator,
      Operator rightChildOperator,
      int bufferSizeInPage) {

    super(outputSchema);
    this.maxSlotNum = bufferSizeInPage * 4096 / 4;
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

    int maxTupleNum = maxSlotNum / tuple.getSize();
    leftTupleBlock = new ArrayList<>();
    leftTupleBlock.add(tuple);

    while (leftTupleBlock.size() < maxTupleNum) {
      tuple = leftChildOperator.getNextTuple();
      if (tuple == null) {
        break;
      }
      leftTupleBlock.add(tuple);
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

    // No more right tuple, return null
    if (this.rightTuple == null) {
      return null;
    }

    // Traversed ALL left tuples, reset left tuple block & load next right tuple
    if (this.leftTupleBlockIndex >= this.leftTupleBlock.size()) {
      if (!loadLeftChildBlock()) {
        this.rightTuple = rightChildOperator.getNextTuple();
        leftChildOperator.reset();
      }
      return getNextTuple();
    }

    return this.leftTupleBlock.get(this.leftTupleBlockIndex++).concat(rightTuple);
  }

  @Override
  public void reset() {
    leftChildOperator.reset();
    rightChildOperator.reset();
  }
}
