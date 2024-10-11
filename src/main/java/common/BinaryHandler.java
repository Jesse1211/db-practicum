package common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

  private File file;
  private FileInputStream fileInputStream;
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;
  private ByteBuffer byteBuffer;

  public BinaryHandler(String tableName) {
    this.file = DBCatalog.getInstance().getFileForTable(tableName);
    this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
  }

  public BinaryHandler(File file) {
    this.file = file;
    this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
  }

  @Override
  public ArrayList<Tuple> readAllTuples() {
    ArrayList<Tuple> tuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = this.readNextTuple()) != null) {
      tuples.add(tuple);
    }
    return tuples;
  }

  /** Read one tuple from a file at a time */
  @Override
  public Tuple readNextTuple() {
    if (this.fileInputStream == null) {
      try {
        this.fileInputStream = new FileInputStream(file);
        this.fileChannel = this.fileInputStream.getChannel();
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    }

    if (this.offset == 0 || this.offset == this.tupleNum * this.attributeNum + 2) {
      if (!loadNextPage()) {
        return null;
      }
    }

    int[] tupleArray = new int[this.attributeNum];
    this.byteBuffer.asIntBuffer().get(this.offset, tupleArray);
    this.offset += this.attributeNum;
    return new Tuple(tupleArray);
  }

  /** Load the next page of the file */
  private boolean loadNextPage() {
    this.byteBuffer.clear();
    try {
      int fileReadLength = fileChannel.read(byteBuffer);
      if (fileReadLength == -1) {
        return false;
      }
      this.byteBuffer.flip();
      this.attributeNum = this.byteBuffer.asIntBuffer().get(0);
      this.tupleNum = this.byteBuffer.asIntBuffer().get(1);
      this.offset = 2;
      return true;
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return false;
  }

  /**
   * Write a tuple to the file
   *
   * @param tuple Tuple to write to the file
   */
  @Override
  public void writeNextTuple(Tuple tuple) {
    if (this.fileOutputStream == null) {
      try {
        file.createNewFile();
        this.fileOutputStream = new FileOutputStream(file);
        this.fileChannel = fileOutputStream.getChannel();
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    }

    if (this.offset == 0) {
      this.attributeNum = tuple.getSize();
      this.byteBuffer.asIntBuffer().put(0, this.attributeNum);
      this.byteBuffer.asIntBuffer().put(1, -1); // placeholder for tuple number
      this.offset = 2;
      this.tupleNum = 0;
    }

    int[] tupleArray = tuple.getAllElementsAsArray();

    this.byteBuffer.asIntBuffer().put(this.offset, tupleArray);
    this.offset += this.attributeNum;
    this.tupleNum++;

    // if next tuple will overflow this page, add a new page.
    if (this.offset + this.attributeNum >= this.bufferCapacity / 4) {
      writePage();
    }
  }

  /** Write the current page to the file, Load the next page */
  private void writePage() {
    try {
      this.byteBuffer.asIntBuffer().put(1, this.tupleNum);

      // Fill the page with 0
      for (int i = offset; i < this.bufferCapacity / 4; i++) {
        this.byteBuffer.asIntBuffer().slice(i, 0);
      }

      fileChannel.write(this.byteBuffer);

      this.byteBuffer.clear(); // Clear buffer for the next load
      this.offset = 0;

    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  @Override
  public void close() {
    try {
      if (this.offset != 0 && this.fileOutputStream != null) {
        writePage();
      }

      if (this.fileOutputStream != null) {
        this.fileOutputStream.close();
      }

      if (this.fileInputStream != null) {
        this.fileInputStream.close();
      }
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

    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }
}
