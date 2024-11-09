package physical_operator;

import common.BinaryHandler;
import common.DBCatalog;
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
   * External Sort constructor, sort the tuples from the child operator by more than one column.
   *
   * @param outputSchema output schema
   * @param childOperator child operator
   * @param orders 2+ orders
   * @param numPagePerBlock number of pages per block
   */
  public ExternalSortOperator(
      ArrayList<Column> outputSchema,
      Operator childOperator,
      List<Column> orders,
      int numPagePerBlock) {

    super(outputSchema);

    this.childOperator = childOperator;
    this.orders = orders;

    int maxTupleNum = numPagePerBlock * 4096 / 4 / childOperator.getOutputSchema().size();
    this.tupleBuffer = new Tuple[maxTupleNum];

    // Divide to files and sort the tuples inside files
    List<File> files = divideAndSort();
    File mergedFile = mergeSortedFiles(files);
    // prepare for reading the sorted tuples
    this.tupleReader = new BinaryHandler(mergedFile);
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

  /**
   * Merge the sorted files. Delete the files on exit.
   *
   * @param externalFileList list of sorted files
   * @return
   */
  private File mergeSortedFiles(List<File> externalFileList) {
    PriorityQueue<Pair<TupleReader, Tuple>> pq =
        new PriorityQueue<>(HelperMethods.getTupleComparator(orders, outputSchema));

    File mergedFile =
        new File(DBCatalog.getInstance().getTempDir() + "/" + UUID.randomUUID() + "sorted.temp");
    mergedFile.deleteOnExit();
    TupleWriter writer = new BinaryHandler(mergedFile);

    // Load top tuple from each file
    for (File file : externalFileList) {
      TupleReader reader = new BinaryHandler(file);
      Tuple tuple = reader.readNextTuple();
      if (tuple != null) {
        pq.offer(new Pair<>(reader, tuple));
      }
    }

    // Get the smallest tuple and write it to the merged file
    // Offer the next tuple from the file to PQ
    while (!pq.isEmpty()) {
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

  /** Write the sorted tuples to temporary file. Delete the file on exit. */
  private File writeTupleBlock() {
    // write the data block to a file

    File file = new File(DBCatalog.getInstance().getTempDir() + "/_" + UUID.randomUUID() + ".temp");
    file.deleteOnExit();
    TupleWriter tupleWriter = new BinaryHandler(file);
    for (Tuple tuple : tupleBuffer) {
      if (tuple == null) break;
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

  @Override
  public void reset(int i) {
    this.tupleReader.reset(i);
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
