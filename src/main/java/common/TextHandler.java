package common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** A class to handle text files and convert them to binary format. */
public class TextHandler implements TupleWriter, TupleReader {

  protected final Logger logger = LogManager.getLogger();
  private BufferedReader bufferedReader;
  private BufferedWriter bufferedWriter;
  private int currentLine = 0;

  /**
   * Use TextHandler to read/write (human-readable) tuples to a (human-readable) file according to
   * tableName
   *
   * @param tableName
   */
  public TextHandler(String tableName) {
    try {
      File file = DBCatalog.getInstance().getFileForTable(tableName);
      this.bufferedReader = new BufferedReader(new FileReader(file));
      this.bufferedWriter = new BufferedWriter(new FileWriter(file, true));
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Use TextHandler to read/write (human-readable) tuples to a (human-readable) file according to
   * file
   *
   * @param file
   */
  public TextHandler(File file) {
    try {
      this.bufferedReader = new BufferedReader(new FileReader(file));
      this.bufferedWriter = new BufferedWriter(new FileWriter(file, true));
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
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
    while ((tuple = readNextTuple()) != null) {
      tuples.add(tuple);
    }
    return tuples;
  }

  /**
   * Read the next tuple from the file
   *
   * @return the next tuple, or null if no more tuples
   */
  @Override
  public Tuple readNextTuple() {
    try {
      String line;
      if ((line = this.bufferedReader.readLine()) != null) {
        currentLine++;
        return new Tuple(line);
      } else {
        bufferedReader.close();
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return null;
  }

  @Override
  public Pair<Tuple, Pair<Integer, Integer>> readNextTupleAndRid() {
    int lineNumber = currentLine;
    Tuple tuple = readNextTuple();
    if (tuple == null) return null;
    return new Pair<>(tuple, new Pair<>(0, lineNumber));
  }

  /**
   * Write a tuple to the file
   *
   * @param tuple the tuple to write to the file for one row
   */
  @Override
  public void writeNextTuple(Tuple tuple) {
    try {
      if (tuple == null) {
        return;
      }
      this.bufferedWriter.write(tuple.toString() + "\n");
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Close the file
   *
   * @throws IOException caused by buffer reader
   */
  @Override
  public void close() {
    try {
      bufferedReader.close();
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Reset the file
   *
   * @throws IOException caused by buffer reader
   */
  @Override
  public void reset() {
    try {
      this.bufferedReader.reset();
      this.currentLine = 0;
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public void reset(int i) {
    this.currentLine = i;
  }

  @Override
  public void reset(int pageIndex, int tupleIndex) {
    this.currentLine = tupleIndex;
  }
}
