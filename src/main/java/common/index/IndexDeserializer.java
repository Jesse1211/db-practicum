package common.index;

import common.pair.Pair;
import common.tuple.Tuple;
import common.tuple.TupleReader;
import compiler.DBCatalog;
import io_handler.BinaryHandler;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IndexDeserializer {
  private TupleReader tupleReader;

  private boolean isClustered;
  private boolean isLoaded = false;
  private final int bufferCapacity;
  private final int attributeIndex;
  private final int lowKey;
  private final int highKey;

  private File file;
  private FileInputStream fileInputStream;
  private FileChannel fileChannel;
  private ByteBuffer byteBuffer;

  private int offset;
  private int nodeId;
  private int numKeys;
  private int entryKey;
  private int ridCount;
  private int leafNodeNum;

  /**
   * IndexDeserializer constructor
   *
   * @param lowKey low key of the range
   * @param highKey high key of the range
   */
  public IndexDeserializer(
      int lowKey,
      int highKey,
      String relationName,
      String attributeName,
      boolean isClustered,
      int attributeIndex) {
    this.isClustered = isClustered;
    this.tupleReader = new BinaryHandler(relationName);
    this.file = DBCatalog.getInstance().getFileForIndex(relationName, attributeName);
    this.bufferCapacity = DBCatalog.getInstance().getBufferCapacity();
    this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
    this.lowKey = lowKey;
    this.highKey = highKey;
    this.attributeIndex = attributeIndex;
    loadNodeById(loadHeaderNode());
  }

  public static int getNumLeaves(String relationName, String attributeName) {
    try {
      File file = DBCatalog.getInstance().getFileForIndex(relationName, attributeName);
      FileInputStream fileInputStream = new FileInputStream(file);

      FileChannel fileChannel = fileInputStream.getChannel();
      fileChannel.position(0);

      ByteBuffer byteBuffer = ByteBuffer.allocate(8);
      byteBuffer.clear();
      fileChannel.read(byteBuffer);
      byteBuffer.flip();
      int numLeaves = byteBuffer.asIntBuffer().get(1);

      fileChannel.close();
      fileInputStream.close();
      return numLeaves;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return -1;
  }

  /**
   * Load 3 integers in header node:
   *
   * <p>Address of the root
   *
   * <p>Number of leaf nodes
   *
   * <p>Tree order: number of keys in the node
   */
  private int loadHeaderNode() {
    try {
      if (this.fileInputStream == null) {
        this.fileInputStream = new FileInputStream(file);
        this.fileChannel = fileInputStream.getChannel();
      }
      this.fileChannel.position(0);
      this.byteBuffer.clear();
      this.fileChannel.read(byteBuffer);
      this.byteBuffer.flip();

      this.leafNodeNum = this.byteBuffer.asIntBuffer().get(1);
      return this.byteBuffer.asIntBuffer().get(0);
      //      this.treeOrder = this.byteBuffer.asIntBuffer().get(2);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return -1;
  }

  /**
   * Load a new node from the index file
   *
   * <p>Flag to indicate if index or leaf node
   */
  private void loadNodeById(int nodeId) {
    try {
      this.byteBuffer.clear();
      this.fileChannel.position(nodeId * bufferCapacity);
      this.fileChannel.read(byteBuffer);
      this.byteBuffer.flip();
      this.nodeId = nodeId;
      // isIndexNode-- 1: index node, 0: leaf node
      if (this.byteBuffer.asIntBuffer().get(0) == 1) {
        loadIndexNode();
      } else {
        loadLeafNode();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Load the index node:
   *
   * <p>Number of keys in the node
   *
   * <p>actual keys in the node, in order
   *
   * <p>Address of the child nodes
   */
  private void loadIndexNode() {
    int numKeys = this.byteBuffer.asIntBuffer().get(1);

    // find the key in the index node
    int left = 2;
    int right = 2 + numKeys;

    while (left < right) {
      int mid = left + (right - left) / 2;
      int key = this.byteBuffer.asIntBuffer().get(mid);

      // if lowKey is valid, find the leftmost key that is greater than or equal to
      // lowKey
      if (key <= this.lowKey) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    // find the address of the child and load the child node
    // left - 1 + numKeys + 1 = left + numKeys
    loadNodeById(this.byteBuffer.asIntBuffer().get(left + numKeys));
  }

  /**
   * Load the leaf node:
   *
   * <p>Number of data entries in the node
   *
   * <p>Load the first entry List
   *
   * <p>
   */
  private void loadLeafNode() {
    this.numKeys = this.byteBuffer.asIntBuffer().get(1);
    this.offset = 2;
    this.ridCount = 0;
  }

  /** After loaded the leaf node, load the next entry list from the leaf node */
  private void loadNextEntry() {
    while (this.numKeys > 0) {
      this.entryKey = this.byteBuffer.asIntBuffer().get(offset++);
      this.ridCount = this.byteBuffer.asIntBuffer().get(offset++);
      this.numKeys--;

      if (this.entryKey >= lowKey) {
        return;
      }
      // go to next entry's offset
      this.offset += this.ridCount * 2;
    }
  }

  public void reset() {
    try {
      // Reset the file position to the start of the file
      fileChannel.position(0);

      // Clear the byteBuffer to make it ready for new data
      this.byteBuffer.clear();

      // reset tuple reader
      this.tupleReader.reset();

      // Reset the offset
      this.offset = 0;

      // Load header node
      loadNodeById(loadHeaderNode());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the next tuple from the index file
   *
   * @return the next tuple
   */
  public Tuple next() {
    // For non-Clustered index, retrieve tuple from the data file
    // For Clustered index, scan the sorted data file sequentially
    if (!this.isClustered || !this.isLoaded) {
      if (this.numKeys == 0 && this.ridCount == 0) {
        if (this.nodeId == leafNodeNum) return null;
        loadNodeById(this.nodeId + 1);
      }
      if (this.ridCount == 0) {
        loadNextEntry();
      }
      this.ridCount--;

      Pair<Integer, Integer> pair =
          new Pair<>(
              this.byteBuffer.asIntBuffer().get(offset++),
              this.byteBuffer.asIntBuffer().get(offset++));

      this.isLoaded = true;
      this.tupleReader.reset(pair.getLeft(), pair.getRight());
    }
    Tuple tuple = this.tupleReader.readNextTuple();

    // key > high key return null else return next tuple\
    if (tuple != null && tuple.getElementAtIndex(attributeIndex) > highKey) {
      return null;
    }
    return tuple;
  }
}
