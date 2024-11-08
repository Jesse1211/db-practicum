package common.tree;

import common.Pair;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class LeafNode extends TreeNode {
  private List<Entry<Integer, List<Pair<Integer, Integer>>>> entries = new ArrayList<>();
  private final NodeType nodeType = NodeType.LEAF_NODE;

  public void setEntry(Entry<Integer, List<Pair<Integer, Integer>>> entry) {
    this.keys.add(entry.getKey());
    this.entries.add(entry);
  }

  @Override
  public int getFirstKey() {return keys.getFirst();}

  @Override
  public List<Integer> getActualKeys(){
    return this.keys;
  }

  /**
   * dump to buffer
   */
  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.clear();
    int offset = 0;

    // the integer 1 as a flag to indicate this is an index node (rather than a leaf node)
    buffer.asIntBuffer().put(offset++, this.nodeType.Value);

    // the number of keys in the node
    buffer.asIntBuffer().put(offset++, this.entries.size());

    // the actual keys in the node, in order
    for(Entry<Integer, List<Pair<Integer, Integer>>> entry: entries) {
      int key = entry.getKey();
      List<Pair<Integer, Integer>> ridList = entry.getValue();

      buffer.asIntBuffer().put(offset++, key);
      buffer.asIntBuffer().put(offset++, ridList.size());
      for (Pair<Integer, Integer> rid : ridList) {
        buffer.asIntBuffer().put(offset++, rid.getLeft());
        buffer.asIntBuffer().put(offset++, rid.getRight());
      }
    }

    // Fill the rest of the page with 0
    for (int i = offset; i < buffer.capacity() / 4; i++) {
      buffer.asIntBuffer().put(i, 0);
    }
  }
}
