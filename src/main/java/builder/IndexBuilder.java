package builder;

import common.HelperMethods;
import common.pair.Pair;
import common.tree.IndexNode;
import common.tree.LeafNode;
import common.tree.TreeNode;
import common.tuple.Tuple;
import common.tuple.TupleReader;
import compiler.DBCatalog;
import io_handler.BinaryHandler;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import net.sf.jsqlparser.schema.Column;
import operator_node.OperatorNode;
import operator_node.ScanOperatorNode;
import operator_node.SortOperatorNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physical_operator.Operator;

public class IndexBuilder {

  private static final Logger logger = LogManager.getLogger();

  private String tableName;
  private String attributeName;
  private List<Column> columns;
  private boolean isClustered;
  private int order;
  private int rootIndex;
  private int numLeaf;

  /**
   * IndexBuilder constructor
   *
   * @param tableName
   * @param attributeName
   * @param isClustered
   * @param order
   */
  public IndexBuilder(String tableName, String attributeName, boolean isClustered, int order) {
    this.tableName = tableName;
    this.attributeName = attributeName;
    this.columns = DBCatalog.getInstance().getColumns(tableName);
    this.isClustered = isClustered;
    this.order = order;
  }

  /**
   * Build the index tree
   *
   * @return A list of TreeNode, ordered by [Leaf Nodes..., Index Nodes..., root]
   */
  public List<TreeNode> build() {
    if (isClustered) {
      preprocessClusteredIndex();
    }
    return buildTree();
  }

  /**
   * Write the header of the index file
   *
   * @param buffer ByteBuffer
   */
  public void writeHeader(ByteBuffer buffer) {
    buffer.clear();
    int offset = 0;

    // index of root
    buffer.asIntBuffer().put(offset++, this.rootIndex);

    // num of leaves
    buffer.asIntBuffer().put(offset++, this.numLeaf);

    // order or tree
    buffer.asIntBuffer().put(offset, this.order);
  }

  /** Sort the data and replace original relation file */
  private void preprocessClusteredIndex() {
    // scan first, then sort by the attribute
    Column attributeColumn =
        columns.stream()
            .filter(c -> c.getColumnName().equals(attributeName))
            .findFirst()
            .orElse(null);

    OperatorNode operatorNode = new ScanOperatorNode(attributeColumn.getTable());
    operatorNode = new SortOperatorNode(operatorNode, Collections.singletonList(attributeColumn));

    // get physical operator
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder();
    operatorNode.accept(physicalPlanBuilder);
    Operator operator = physicalPlanBuilder.getResult();

    // output the data
    try {
      File outfile = DBCatalog.getInstance().getFileForTable(this.tableName);
      outfile.createNewFile();
      operator.dump(new BinaryHandler(outfile));
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Get all entries in the table
   *
   * @return TreeMap<Integer, List<Pair<Integer, Integer>>>
   */
  private TreeMap<Integer, List<Pair<Integer, Integer>>> getAllEntries() {
    TreeMap<Integer, List<Pair<Integer, Integer>>> treeMap = new TreeMap<>();
    TupleReader tupleReader = new BinaryHandler(this.tableName);
    Map<String, Integer> columnIndexMap = HelperMethods.mapColumnIndex(columns);

    Pair<Tuple, Pair<Integer, Integer>> tupleWithRid;
    while ((tupleWithRid = tupleReader.readNextTupleAndRid()) != null) {
      int attributeIndex = columnIndexMap.get(tableName + "." + attributeName);
      int key = tupleWithRid.getLeft().getElementAtIndex(attributeIndex);
      if (!treeMap.containsKey(key)) {
        treeMap.put(key, new ArrayList<>());
      }
      treeMap.get(key).add(tupleWithRid.getRight());
    }
    return treeMap;
  }

  /**
   * build tree in the order by using the treemap
   *
   * @return A list of TreeNode, ordered by [Leaf Nodes..., Index Nodes..., root]
   */
  private List<TreeNode> buildTree() {
    List<Entry<Integer, List<Pair<Integer, Integer>>>> entryList =
        new ArrayList<>(getAllEntries().entrySet());
    List<TreeNode> nodes = new ArrayList<>();
    int nodeIndex = 1;

    int childIndex = 0;
    int childCount = entryList.size();

    while (childIndex < childCount) {
      int numIter;
      if (2 * order < childCount - childIndex && childCount - childIndex < 3 * order) {
        // split them into half
        numIter = (childCount - childIndex) / 2;
      } else {
        // iterate at most 2*order times, or the number left.
        numIter = Math.min(2 * order, childCount - childIndex);
      }

      // create a new LeafNode, add the data entries.
      LeafNode node = new LeafNode();

      // assign index, then increment
      node.index = nodeIndex++;
      while (numIter > 0) {
        Entry<Integer, List<Pair<Integer, Integer>>> entry = entryList.get(childIndex);
        node.setEntry(entry);
        numIter--;
        childIndex++;
      }
      nodes.add(node);
    }

    this.numLeaf = nodes.size();

    /*
     * At this point, we have parsed all the leaf nodes.
     * Continue to build all index nodes using the leaf nodes we have.
     */
    childIndex = 0;
    // if current layer is more than 2*order + 1, that means we need to have
    // additional layers before setting the root.
    while (nodes.size() - childIndex > 2 * order + 1) {
      childCount = nodes.size();
      while (childIndex < childCount) {
        int numIter;
        if (2 * order + 1 < childCount - childIndex && childCount - childIndex < 3 * order + 2) {
          numIter = (childCount - childIndex) / 2;
        } else {
          numIter = Math.min(2 * order + 1, childCount - childIndex);
        }

        IndexNode node = new IndexNode();
        node.index = nodeIndex++;
        while (numIter > 0) {
          TreeNode child = nodes.get(childIndex);
          node.setChild(child);
          numIter--;
          childIndex++;
        }
        nodes.add(node);
      }
    }

    // set the root
    IndexNode node = new IndexNode();
    node.index = nodeIndex;
    this.rootIndex = nodeIndex;
    while (childIndex < nodes.size()) {
      TreeNode child = nodes.get(childIndex);
      node.setChild(child);
      childIndex++;
    }
    nodes.add(node);

    return nodes;
  }
}
