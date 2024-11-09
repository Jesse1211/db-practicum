package common.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IndexNode extends TreeNode {
  private List<Integer> keys = new ArrayList<>();
  private List<TreeNode> children = new ArrayList<>();
  private final NodeType nodeType = NodeType.INDEX_NODE;

  public void setChild(TreeNode child) {
    this.keys.add(child.getFirstKey());
    this.children.add(child);
  }

  @Override
  public int getFirstKey() {
    return keys.getFirst();
  }

  @Override
  public List<Integer> getActualKeys() {
    return this.keys.subList(1, this.keys.size());
  }

  public List<TreeNode> getChildren() {
    return children;
  }

  private int getActualKeySize() {
    return this.keys.size() - 1;
  }

  /** dump to buffer */
  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.clear();
    int offset = 0;

    // the integer 1 as a flag to indicate this is an index node (rather than a leaf node)
    buffer.asIntBuffer().put(offset++, this.nodeType.Value);

    // the number of keys in the node
    buffer.asIntBuffer().put(offset++, getActualKeySize());

    // the actual keys in the node, in order
    for (int key : getActualKeys()) {
      buffer.asIntBuffer().put(offset++, key);
    }

    // the addresses of all the children of the node, in order
    for (TreeNode child : getChildren()) {
      buffer.asIntBuffer().put(offset++, child.index);
    }

    // Fill the rest of the page with 0
    for (int i = offset; i < buffer.capacity() / 4; i++) {
      buffer.asIntBuffer().put(i, 0);
    }
  }
}
