package physical_operator;

import common.BinaryHandler;
import common.HelperMethods;
import common.Tuple;
import common.TupleReader;
import common.TupleWriter;
import java.io.File;
import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * An operator for ORDER BY. Create intermediate files to keep the partial sorted runs which are
 * pending a merge in the next pass. Will clean up the intermediate files after the query is done.
 *
 * <p>Intermediate files are binary for production, human-readable for debugging.
 */
public class ExternalSortOperator extends Operator {
  private Map<String, Integer> columnIndexMap;
  private List<OrderByElement> elementOrders;
  private String tempDir = "temp";
  private int maxSlotNum;
  private int blockIndex;
  private Operator operator;
  private Tuple[] tupleArray;
  private List<TupleReader> blockReaders;
  private Tuple[] tupleBuffer;

  /**
   * SelectOperator constructor
   *
   * @param operator scan | select | join operator
   * @param elementOrders list of ORDER BY elements
   */
  public ExternalSortOperator(
      ArrayList<Column> outputSchema,
      Operator operator,
      List<OrderByElement> elementOrders,
      int bufferSizeInPage) {
    super(outputSchema);
    this.maxSlotNum = bufferSizeInPage * 4096 / 4;
    this.columnIndexMap = HelperMethods.mapColumnIndex(operator.getOutputSchema());
    this.elementOrders = elementOrders;
    this.blockReaders = new ArrayList<>();
    this.blockIndex = 0;
    this.operator = operator;
    while (extractDataBlock()) {
      sort();
      writeDataBlock();
    }
    // 做一个heap for buffer
    // merge
  }

  /** Merge the sorted files */
  private void merge() {}

  /**
   * Extract a block of data from the child operator.
   *
   * @return true if there are tuples to extract, false otherwise
   */
  private boolean extractDataBlock() {
    Tuple tuple = operator.getNextTuple();

    if (tuple == null) {
      return false;
    }

    int maxTupleNum = maxSlotNum / tuple.getSize();
    tupleArray = new Tuple[maxTupleNum];
    int index = 1;

    while (index < maxTupleNum) {
      tuple = operator.getNextTuple();
      if (tuple == null) {
        break;
      }
      tupleArray[index++] = tuple;
    }
    return true;
  }

  /** Write the sorted tuples to temporary file. */
  private void writeDataBlock() {
    // write the data block to a file
    String fileName = blockIndex + ".txt";
    File file = new File(fileName);
    TupleWriter tupleWriter = new BinaryHandler(file);
    this.blockReaders.add(new BinaryHandler(file));
    for (Tuple tuple : tupleArray) {
      if (tuple == null) {
        break;
      }
      tupleWriter.writeNextTuple(tuple);
    }
    blockIndex++;
  }

  /**
   * Sort the tuples based on the column specified in the ORDER BY clause. Then sort the tuples
   * based on the subsequent columns to break ties.
   */
  private void sort() {
    Arrays.sort(
        tupleArray,
        new Comparator<Tuple>() {
          @Override
          public int compare(Tuple t1, Tuple t2) {
            if (t1 == null && t2 == null) {
              return 0;
            } else if (t1 == null) {
              return 1;
            } else if (t2 == null) {
              return -1;
            }

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

  /** Reset the TupleReader */
  @Override
  public void reset() {
    for (TupleReader tupleReader : blockReaders) {
      tupleReader.reset();
    }
  }

  /**
   * Merge the sorted files and return the next tuple.
   *
   * @return individual tuples from the child operator's all tuples
   */
  @Override
  public Tuple getNextTuple() {
    if (blockReaders.size() == 0) {
      return null;
    }

    Tuple minTuple = null;
    int minIndex = -1;
    for (int i = 0; i < this.tupleBuffer.length; i++) {

      // if the buffer is empty, read the next tuple
      if (tupleBuffer[i] == null) {
        tupleBuffer[i] = blockReaders.get(i).readNextTuple();

        // if the buffer is still empty, this file is done
        if (tupleBuffer[i] == null) {
          continue;
        }
      }

      // initialize the minTuple
      if (minTuple == null) {
        minTuple = tupleBuffer[i];
        minIndex = i;
      } else {
        // compare the tuple with the minTuple
        for (OrderByElement elementOrder : elementOrders) {
          Column column = (Column) elementOrder.getExpression();
          int index = columnIndexMap.get(column.getName(true));
          int compare =
              Integer.compare(
                  minTuple.getElementAtIndex(index), tupleBuffer[i].getElementAtIndex(index));
          if (compare != 0) {
            if (compare > 0) {
              minTuple = tupleBuffer[i];
              minIndex = i;
            }
            break;
          }
        }
      }
    }

    // if the minTuple is not null, remove it from the buffer
    if (minTuple != null && minIndex != -1) {
      tupleBuffer[minIndex] = blockReaders.get(minIndex).readNextTuple();
    }

    return minTuple;
  }
}
