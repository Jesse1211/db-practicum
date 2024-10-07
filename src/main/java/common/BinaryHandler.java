package common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** A class to handle binary files. It reads and writes tuples to the file. */
public class BinaryHandler implements TupleWriter, TupleReader {
  protected final Logger logger = LogManager.getLogger();
  private final int bufferCapacity = 4096;
  private int attributeNum;
  private int tupleNum;
  private int offset;

  private FileInputStream fileInputStream;
  private FileChannel fileChannel;
  private ByteBuffer byteBuffer;

  public BinaryHandler(String tableName) {
    try {
      File file = DBCatalog.getInstance().getFileForTable(tableName);
      this.fileInputStream = new FileInputStream(file);
      this.fileChannel = fileInputStream.getChannel();
      this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
      this.offset = 0;
      loadNextPage();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  public BinaryHandler(File file) {
    try {
      this.fileInputStream = new FileInputStream(file);
      this.fileChannel = fileInputStream.getChannel();
      this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
      this.offset = 0;
      loadNextPage();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public ArrayList<Tuple> readAllTuples() {
    Tuple tuple;
    ArrayList<Tuple> tuples = new ArrayList<>();
    while ((tuple = this.readNextTuple()) != null) {
      tuples.add(tuple);
    }
    return tuples;
  }

  /** Read one tuple from a file at a time */
  @Override
  public Tuple readNextTuple() {
    if (this.offset == 0 || this.tupleNum == 0 || this.attributeNum == 0) {
      return null;
    }

    if (this.offset == this.tupleNum * this.attributeNum + 2) {
      if (!loadNextPage()) {
        return null;
      }
    }

    int tupleSize = this.attributeNum;
    int[] tupleArray = new int[tupleSize];
    this.byteBuffer.asIntBuffer().get(this.offset, tupleArray);
    this.offset += tupleSize;
    return new Tuple(tupleArray);
  }

  /** Load the next page of the file */
  private boolean loadNextPage() {
    this.byteBuffer.clear();
    this.offset = 0;
    try {
      int fileReadLength = fileChannel.read(byteBuffer);
      if (fileReadLength == -1) {
        return false;
      }
      this.byteBuffer.flip();
      this.attributeNum = this.byteBuffer.asIntBuffer().get(0);
      this.tupleNum = this.byteBuffer.asIntBuffer().get(1);
      this.offset += 2;
      return true;
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return true;
  }

  /**
   * Write a tuple to the file
   *
   * @param tuple Tuple to write to the file
   */
  @Override
  public void writeTuple(Tuple tuple) {
    // TODO: 如果没满但是不够放下这个tuple???
    if (this.offset == this.bufferCapacity) {
      writeNextPage();
    }

    int[] tupleArray = tuple.getAllElementsAsArray();

    this.byteBuffer.asIntBuffer().put(attributeNum, tupleArray);
    this.offset += tupleArray.length * 4;
  }

  /** Write the current page to the file, Load the next page */
  private void writeNextPage() {
    this.byteBuffer.flip();
    try {
      fileChannel.write(byteBuffer);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    this.byteBuffer.clear();
    this.offset = 0;
    try {
      int fileReadLength = fileChannel.read(byteBuffer);
      if (fileReadLength == -1) {
        return;
      }
      this.byteBuffer.flip();
      this.attributeNum = this.byteBuffer.asIntBuffer().get(0);
      this.tupleNum = this.byteBuffer.asIntBuffer().get(1);
      this.offset += 4 * 2;
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public void close() {
    try {
      this.fileInputStream.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public void reset() {
    try {
      // Reset the file position to the start of the file
      fileChannel.position(0);

      // Clear the byteBuffer to make it ready for new data
      this.byteBuffer.clear();

      // Reset the offset
      this.offset = 0;

      // Load the first page after reset
      loadNextPage();

    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }
}
