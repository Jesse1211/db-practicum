package physical_operator;

import common.HelperMethods;
import common.stats.StatsInfo;
import common.tuple.Tuple;
import common.tuple.TupleWriter;
import compiler.DBCatalog;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import net.sf.jsqlparser.schema.Column;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract class to represent relational operators for physical query plans. Every operator has a
 * reference to an outputSchema which represents the schema of the output tuples from the operator.
 * This is a list of Column objects. Each Column has an embedded Table object with the name and
 * alias (if required) fields set appropriately.
 */
public abstract class Operator {

  protected final Logger logger = LogManager.getLogger();
  protected ArrayList<Column> outputSchema;

  /**
   * Constructor for Operator
   *
   * @param outputSchema schema of the output tuples
   */
  public Operator(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
  }

  /**
   * Get the output schema of the operator
   *
   * @return output schema of the operator
   */
  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  /** Resets cursor on the operator to the beginning */
  public abstract void reset();

  /** Reset cursor on the operator to ith tuple */
  public void reset(int i) throws NotImplementedException {}

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public abstract Tuple getNextTuple();

  /**
   * Collects all tuples of this operator.
   *
   * @return A list of Tuples.
   */
  public List<Tuple> getAllTuples() {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }

    return tuples;
  }

  /**
   * Iterate through output of operator and send it all to the specified printStream)
   *
   * @param printStream stream to receive output, one tuple per line.
   */
  public void dump(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }

  /**
   * Iterate through output of operator and send it all to the specified file by tupleWriter
   *
   * @param tupleWriter TupleWriter to receive output
   */
  public void dump(TupleWriter tupleWriter) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      tupleWriter.writeNextTuple(t);
    }
    tupleWriter.close();
  }

  /**
   * Load a block of tuples from the operator into the buffer
   *
   * @param operator
   * @param buffer
   * @return true if there are more tuples loaded, false otherwise
   */
  protected static boolean loadTupleBlock(Operator operator, Tuple[] buffer) {
    if (buffer.length == 0) {
      return false;
    }

    // Return FALSE if there are no more tuples
    Tuple tuple = operator.getNextTuple();
    if (tuple == null) {
      return false;
    }

    buffer[0] = tuple;
    int i = 1;
    for (; i < buffer.length; i++) {
      tuple = operator.getNextTuple();

      // Stop loading when there are no more tuples
      if (tuple == null) break;
      buffer[i] = tuple;
    }

    // empty the rest of the buffer
    for (; i < buffer.length; i++) {
      buffer[i] = null;
    }
    return true;
  }

  /** Print the physical query plan in term of a tree structure */
  public StringBuilder print() {
    StringBuilder tree = new StringBuilder();
    dfs(tree, this, 0);
    return tree;
  }

  /**
   * Depth first search to print the logical query plan in term of a tree structure
   *
   * @param tree the tree structure to store the logical query plan
   * @param cur the current operator node
   * @param level the current level of the tree
   */
  private void dfs(StringBuilder tree, Operator cur, int level) {

    for (int i = 0; i < level; i++) {
      tree.append("-");
    }

    if (cur instanceof DuplicateEliminationOperator) {
      tree.append("DupElim\n");
      dfs(tree, ((DuplicateEliminationOperator) (cur)).getChildOperator(), level + 1);
    } else if (cur instanceof ExternalSortOperator) {
      tree.append("ExternalSort[");
      tree.append(HelperMethods.convertColumnList(((ExternalSortOperator) cur).getOrders()))
          .append("]\n");
      dfs(tree, ((ExternalSortOperator) (cur)).getChildOperator(), level + 1);
    } else if (cur instanceof ProjectOperator) {
      tree.append("Project[")
          .append(HelperMethods.convertColumnList(cur.getOutputSchema()))
          .append("]\n");
      dfs(tree, ((ProjectOperator) (cur)).getChildOperator(), level + 1);
    } else if (cur instanceof SelectOperator) {
      SelectOperator operator = ((SelectOperator) cur);

      if (operator.getChildOperator() instanceof JoinOperator) {
        tree.append("TNLJ[").append(operator.getWhereExpression().toString()).append("]\n");

        Operator left = ((JoinOperator) operator.getChildOperator()).getLeftOperator();
        Operator right = ((JoinOperator) operator.getChildOperator()).getRightOperator();
        dfs(tree, left, level + 1);
        dfs(tree, right, level + 1);
      } else if (operator.getChildOperator() instanceof BNLJOperator) {
        tree.append("BNLJ[").append(operator.getWhereExpression().toString()).append("]\n");
        Operator left = ((BNLJOperator) operator.getChildOperator()).getLeftOperator();
        Operator right = ((BNLJOperator) operator.getChildOperator()).getRightOperator();
        dfs(tree, left, level + 1);
        dfs(tree, right, level + 1);
      } else {
        tree.append("Select[").append(operator.getWhereExpression().toString()).append("]\n");
        dfs(tree, ((SelectOperator) cur).getChildOperator(), level + 1);
      }

    } else if (cur instanceof SMJOperator) {
      SMJOperator operator = ((SMJOperator) cur);
      tree.append("SMJ[")
          .append(operator.leftColumnName)
          .append(" = ")
          .append(operator.rightColumnName)
          .append("]\n");
      dfs(tree, ((SMJOperator) (cur)).getLeftOperator(), level + 1);
      dfs(tree, ((SMJOperator) (cur)).getRightOperator(), level + 1);
    } else if (cur instanceof ScanOperator) {
      tree.append("TableScan[" + cur.getOutputSchema().get(0).getTable().getName() + "]\n");
    } else if (cur instanceof IndexScanOperator) {
      IndexScanOperator operator = (IndexScanOperator) cur;
      DBCatalog catalog = DBCatalog.getInstance();
      StatsInfo statsInfo = catalog.getStatsInfo(operator.table.getName());
      int low =
          Math.max(statsInfo.columnStats.get(operator.attributeName).getLeft(), operator.lowKey);
      int high =
          Math.min(statsInfo.columnStats.get(operator.attributeName).getRight(), operator.highKey);

      tree.append(
          "IndexScan["
              + operator.table.getName()
              + ", "
              + operator.attributeName
              + ", "
              + low
              + ", "
              + high
              + "]\n");
    } else if (cur instanceof SortOperator) {
      tree.append("Sort[")
          .append(HelperMethods.convertColumnList(((SortOperator) cur).getOrders()))
          .append("]\n");
    }
  }
}
