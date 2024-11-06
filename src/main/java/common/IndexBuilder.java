package common;

import common.tree.IndexNode;
import common.tree.LeafNode;
import common.tree.TreeNode;
import java.io.File;
import java.io.IOException;
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
  private boolean clustered;
  private int order;

  private Column attributeColumn;

  public IndexBuilder(String tableName, String attributeName, boolean clustered, int order) {
    this.tableName = tableName;
    this.attributeName = attributeName;
    this.columns = DBCatalog.getInstance().getColumns(tableName);
    this.clustered = clustered;
    this.order = order;
  }

  public List<TreeNode> build() {
    if (clustered) {
      preprocessClusteredIndex();
    }
    return this.buildTree();
  }

  /**
   * Sort the data and replace original relation file
   */
  private void preprocessClusteredIndex() {
    // scan first, then sort by the attribute
    OperatorNode operatorNode = new ScanOperatorNode(this.attributeColumn.getTable());
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


  private TreeMap<Integer, List<Pair<Integer, Integer>>> getAllEntries() {
    TreeMap<Integer, List<Pair<Integer, Integer>>> treeMap = new TreeMap<>();
    TupleReader tupleReader = new BinaryHandler(this.tableName);
    Map<String, Integer> columnIndexMap = HelperMethods.mapColumnIndex(columns);

    Pair<Tuple, Pair<Integer, Integer>> tupleWithRid;
    while ((tupleWithRid = tupleReader.readNextTupleAndRid()) != null) {
      int attributeIndex = columnIndexMap.get(attributeName);
      int key = tupleWithRid.getLeft().getElementAtIndex(attributeIndex);
      if (!treeMap.containsKey(key)) {
        treeMap.put(key, new ArrayList<>());
      }
      treeMap.get(key).add(tupleWithRid.getRight());
    }
    System.out.println("Total number of unique keys: " + treeMap.size());
    return treeMap;
  }


  /**
   * build tree in the order by using the treemap
   *
   * @return A list of TreeNode, ordered by [Leaf Nodes..., Index Nodes..., root]
   */
  private List<TreeNode> buildTree() {
    List<Entry<Integer, List<Pair<Integer, Integer>>>> entryList = new ArrayList<>(
            getAllEntries().entrySet());
    List<TreeNode> nodes = new ArrayList<>();
    int nodeIndex = 1;

    int index = 0;
    int length = entryList.size();

    while (index < length) {
      int numIter;
      if (2 * order < length - index && length - index < 3 * order) {
        // split them into half
        numIter = (length - index) / 2;
      } else {
        // iterate at most 2*order times, or the number left.
        numIter = Math.min(2 * order, length - index);
      }

      // create a new LeafNode, add the data entries.
      LeafNode node = new LeafNode();

      // assign index, then increment
      node.index = nodeIndex++;
      while (numIter > 0) {
        Entry<Integer, List<Pair<Integer, Integer>>> entry = entryList.get(index);
        node.setEntry(entry);
        numIter--;
        index++;
      }
      nodes.add(node);
    }

    /* At this point, we have parsed all the leaf nodes.
       Continue to build all index nodes using the leaf nodes we have.
     */
    index = 0;
    // if current layer is more than 2*order + 1, that means we need to have additional layers before setting the root.
    while (nodes.size() - index > 2 * order + 1) {
      length = nodes.size();
      while (index < length) {
        int numIter;
        if (2 * order + 1 < length - index && length - index < 3 * order + 2) {
          numIter = (length - index) / 2;
        } else {
          numIter = Math.min(2 * order + 1, length - index);
        }

        IndexNode node = new IndexNode();
        node.index = nodeIndex++;
        while (numIter > 0) {
          TreeNode child = nodes.get(index);
          node.setChild(child);
          numIter--;
          index++;
        }
        nodes.add(node);
      }
    }

    // set the root
    IndexNode node = new IndexNode();
    node.index = nodeIndex;
    while (index < length) {
      TreeNode child = nodes.get(index);
      node.setChild(child);
      index++;
    }
    nodes.add(node);

    return nodes;
  }
}


