package common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileReader;

import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;

/**
 * A class to handle text files and convert them to binary format.
 */
public class TextHandler implements TupleWriter, TupleReader {

    protected final Logger logger = LogManager.getLogger();
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;

    public TextHandler(String tableName) {
        try {
            File file = DBCatalog.getInstance().getFileForTable(tableName);
            this.bufferedReader = new BufferedReader(new FileReader(file));
            this.bufferedWriter = new BufferedWriter(new FileWriter(file, true));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public TextHandler(File file) {
      try {
        this.bufferedReader = new BufferedReader(new FileReader(file));
        this.bufferedWriter = new BufferedWriter(new FileWriter(file, true));
      } catch (IOException e) {
        logger.error(e.getMessage());
      }
    }

  @Override
  public ArrayList<Tuple> readAllTuples() {
    ArrayList<Tuple> tuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = readNextTuple()) != null) {
      tuples.add(tuple);
    }
    return tuples;
  }

    @Override
    public Tuple readNextTuple() {
        try {
            String line;
            if ((line = this.bufferedReader.readLine()) != null) {
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
    public void writeTuple(Tuple tuple) {
        try {
            this.bufferedWriter.write(tuple.toString() + "\n");
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            bufferedReader.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void reset() {
        try {
            this.bufferedReader.reset();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
