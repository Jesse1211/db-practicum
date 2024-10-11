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

/** A class to handle binary files. It reads OR writes (binary) tuples to a (binary) file. */
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

  /**
   * Use BinaryHandler to read/write tuples to a file according to tableName
   *
   * @param tableName
   */
  public BinaryHandler(String tableName) {
    this.file = DBCatalog.getInstance().getFileForTable(tableName);
    this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
  }

  /**
   * Use BinaryHandler to read/write tuples to a file according to file
   *
   * @param file
   */
  public BinaryHandler(File file) {
    this.file = file;
    this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
  }

  /**
   * Read all tuples from a file
   *
   * @return an ArrayList for all tuples
   */
  @Override
  public ArrayList<Tuple> readAllTuples() {
    ArrayList<Tuple> tuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = this.readNextTuple()) != null) {
      tuples.add(tuple);
    }
    return tuples;
  }

  /**
   * Read one tuple from a file at a time. It will return null if there is no more tuple to read.
   * And it will handle the file reading process.
   *
   * @return the row in this file
   * @throws IOException caused by file reading for writing
   */
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

  /**
   * Load the next page of the file for reading, update the attributeNum, tupleNum, and offset
   *
   * @return true if the next page is loaded successfully, false otherwise
   * @throws IOException caused by file reading for writing or byte buffer
   */
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
   * @throws IOException caused by file writing
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

  /**
   * Write the current page to the file, Load the next page by
   *
   * @return void
   * @throws IOException caused by file writing
   */
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

  /**
   * Write potential tuples, Close the file input stream and file output stream
   *
   * @return void
   * @throws IOException caused by file writing / reading / closing
   */
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

  /**
   * Reset the file to the beginning
   *
   * @return void
   * @throws IOException caused by file reading or byte buffer
   */
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
