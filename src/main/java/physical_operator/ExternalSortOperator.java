package physical_operator;

import common.BinaryHandler;
import common.HelperMethods;
import common.Pair;
import common.Tuple;
import common.TupleReader;
import common.TupleWriter;
import java.io.File;
import java.util.*;
import net.sf.jsqlparser.schema.Column;

/**
 * An operator for ORDER BY. Create intermediate files to keep the partial sorted runs which are
 * pending a merge in the next pass. Will clean up the intermediate files after the query is done.
 *
 * <p>Intermediate files are binary for production, human-readable for debugging.
 */
public class ExternalSortOperator extends Operator {
  private Operator childOperator;
  private List<Column> orders;
  private Tuple[] tupleBuffer;
  private TupleReader tupleReader;

  /**
   * SelectOperator constructor
   *
   * @param childOperator scan | select | join operator
   * @param orders list of ORDER BY elements
   */
  public ExternalSortOperator(
          ArrayList<Column> outputSchema,
          Operator childOperator,
          List<Column> orders,
          int numPagePerBlock
  ) {

    super(outputSchema);

    this.childOperator = childOperator;
    this.orders = orders;

    int maxTupleNum = numPagePerBlock * 4096 / 4 / childOperator.getOutputSchema().size();
    this.tupleBuffer = new Tuple[maxTupleNum];

    List<File> files = divideAndSort();
    File mergedFile = mergeSortedFiles(files);
    this.tupleReader = new BinaryHandler(mergedFile);
  }

  public ExternalSortOperator(
      ArrayList<Column> outputSchema,
      Operator childOperator,
      Column order,
      int numPagePerBlock) {
    this(outputSchema, childOperator, Collections.singletonList(order), numPagePerBlock);
  }

  /**
   * For each block, Load, sort then write the tuples temporary files.
   * 
   * @return
   */
  private List<File> divideAndSort() {
    List<File> externalFileList = new ArrayList<>();

    while (Operator.loadTupleBlock(childOperator, tupleBuffer)) {
      // sort the tuples
      Arrays.sort(tupleBuffer, HelperMethods.getTupleComparator(orders, outputSchema));
      File file = writeTupleBlock();
      externalFileList.add(file);
    }
    return externalFileList;
  }

  private File mergeSortedFiles(List<File> externalFileList){
    PriorityQueue<Pair<TupleReader, Tuple>> pq = new PriorityQueue<>(
            HelperMethods.getTupleComparator(orders, outputSchema)
    );

    File mergedFile = new File("_" + UUID.randomUUID() + "sorted.temp");
    mergedFile.deleteOnExit();
    TupleWriter writer = new BinaryHandler(mergedFile);

    for(File file: externalFileList){
      TupleReader reader = new BinaryHandler(file);
      Tuple tuple = reader.readNextTuple();
      if (tuple != null){
        pq.offer(new Pair<>(reader, tuple));
      }
    }

    while (!pq.isEmpty()){
      Pair<TupleReader, Tuple> pair = pq.poll();
      writer.writeNextTuple(pair.getRight());

      TupleReader reader = pair.getLeft();
      Tuple tuple = reader.readNextTuple();
      if (tuple != null) {
        pq.offer(new Pair<>(reader, tuple));
      }
    }
    writer.close();

    return mergedFile;
  }

  /** Write the sorted tuples to temporary file. */
  private File writeTupleBlock() {
    // write the data block to a file
    File file = new File("_" + UUID.randomUUID() + ".temp");
    TupleWriter tupleWriter = new BinaryHandler(file);
    for (Tuple tuple : tupleBuffer) {
      if (tuple == null)
        break;
      tupleWriter.writeNextTuple(tuple);
    }
    tupleWriter.close();
    return file;
  }



  /** Reset the TupleReader */
  @Override
  public void reset() {
    this.tupleReader.reset();
  }

  /**
   * Merge the sorted files and return the next tuple.
   *
   * @return individual tuples from the child operator's all tuples
   */
  @Override
  public Tuple getNextTuple() {
    return this.tupleReader.readNextTuple();
  }
}
