package common.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class TreeNode {
  enum NodeType{
    LEAF_NODE(0),
    INDEX_NODE(1);

    public final int Value;
    NodeType(int value){
      Value = value;
    }
  }

  public int index;
  protected ArrayList<Integer> keys = new ArrayList<>();


  public abstract List<Integer> getActualKeys();

  public abstract void serialize(ByteBuffer buffer);
}
