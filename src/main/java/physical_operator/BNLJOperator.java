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
  Iterator<Tuple> it;

  private int bufferSizeByte;
  private Operator leftChildOperator;
  private Operator rightChildOperator;

  public BNLJOperator(
      ArrayList<Column> outputSchema,
      Operator leftChildOperator,
      Operator rightChildOperator,
      int bufferSizeInPage) {

    super(outputSchema);
    this.bufferSizeByte = bufferSizeInPage * 4096; // in Byte
    this.leftChildOperator = leftChildOperator;
    this.rightChildOperator = rightChildOperator;
  }

  private void loadLeftChildBlock() {
    Tuple tuple = leftChildOperator.getNextTuple();
    int maxTupleNum = bufferSizeByte / (tuple.getSize() * 4);
    leftTupleBlock = new ArrayList<>();
    leftTupleBlock.add(tuple);

    int blockByteSize = 1;

    while (blockByteSize < maxTupleNum) {
      tuple = leftChildOperator.getNextTuple();
      if (tuple == null) {
        break;
      }
      leftTupleBlock.add(tuple);
      blockByteSize += tuple.getSize();
    }
    it = leftTupleBlock.iterator();
  }

  /**
   * Output one tuple at a time, block buffer can be resumed. Traverse ALL right tuples for each
   * left tuple in the block.
   *
   * @return a tuple that glues left and right tuples
   */
  @Override
  public Tuple getNextTuple() {

    if (!it.hasNext()) { // Re-Traverse left block
      it = leftTupleBlock.iterator();
    }

    Tuple rightTuple = rightChildOperator.getNextTuple();

    if (rightTuple == null) { // Traversed ALL right tuples, move to next left tuple block
      rightChildOperator.reset();
      loadLeftChildBlock();
      return getNextTuple();
    }

    return it.next().concat(rightTuple);
  }

  @Override
  public void reset() {
    leftChildOperator.reset();
    rightChildOperator.reset();
    loadLeftChildBlock();
  }
}
