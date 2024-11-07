package common;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IndexDeserializer {
  private TupleReader tupleReader;

  private boolean isClustered;
  private boolean isLoaded = false;
  private int bufferCapacity;

  private File file;
  private FileInputStream fileInputStream;
  private FileChannel fileChannel;
  private ByteBuffer byteBuffer;

  private int lowKey;
  private int highKey;

  // content of header page
  private int treeOrder;
  private int leafNodeNum;

  private int offset;
  private int count;
  private int value;
  private int numKeys;
  private int rootOffset;

  /**
   * IndexDeserializer constructor
   *
   * @param lowKey low key of the range
   * @param highKey high key of the range
   */
  public IndexDeserializer(int lowKey, int highKey, String tableName) {
    IndexInfo indexInfo = DBCatalog.getInstance().getIndexInfo(tableName);
    this.isClustered = indexInfo.isClustered;

    this.tupleReader = new BinaryHandler(tableName);
    this.file =
        DBCatalog.getInstance().getFileForIndex(indexInfo.relationName, indexInfo.attributeName);
    this.bufferCapacity = DBCatalog.getInstance().getBufferCapacity();
    this.byteBuffer = ByteBuffer.allocate(bufferCapacity);
    this.lowKey = lowKey;
    this.highKey = highKey;
    loadHeaderPage();
    loadPageByOffset(this.rootOffset);
  }

  /**
   * Load 3 integers in header page:
   *
   * <p>Address of the root
   *
   * <p>Number of leaf nodes
   *
   * <p>Tree order: number of keys in the node
   */
  private void loadHeaderPage() {
    try {
      this.fileInputStream = new FileInputStream(file);
      this.fileChannel = fileInputStream.getChannel();
      this.byteBuffer.clear();
      this.fileChannel.read(byteBuffer);
      this.byteBuffer.flip();

      this.rootOffset = this.byteBuffer.asIntBuffer().get(0);
      this.leafNodeNum = this.byteBuffer.asIntBuffer().get(1);
      this.treeOrder = this.byteBuffer.asIntBuffer().get(2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Load a new page from the index file
   *
   * <p>Flag to indicate if index or leaf node
   */
  private void loadPageByOffset(int offset) {
    try {
      this.byteBuffer.clear();
      this.fileChannel.position(offset * 4096);
      this.fileChannel.read(byteBuffer);
      this.byteBuffer.flip();

      boolean isIndexNode = this.byteBuffer.asIntBuffer().get(0) == 1;
      if (isIndexNode) {
        loadIndexPage();
      } else {
        loadLeafPage();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Load the index page:
   *
   * <p>Number of keys in the node
   *
   * <p>actual keys in the node, in order
   *
   * <p>Address of the child nodes
   */
  private void loadIndexPage() {
    int numKeys = this.byteBuffer.asIntBuffer().get(1);

    // find the key in the index node
    int left = 2;
    int right = 2 + numKeys;
    if (this.byteBuffer.asIntBuffer().get(left) < this.lowKey) {
      loadPageByOffset(this.byteBuffer.asIntBuffer().get(left + numKeys));
      return;
    }

    while (left < right) {
      int mid = left + (right - left) / 2;
      int key = this.byteBuffer.asIntBuffer().get(mid);

      // if lowKey is valid, find the leftmost key that is greater than or equal to
      // lowKey
      if (key < this.lowKey) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    // find the address of the child and load the child page
    loadPageByOffset(this.byteBuffer.asIntBuffer().get(left + numKeys + 1));
  }

  /**
   * Load the leaf page:
   *
   * <p>Number of data entries in the node
   *
   * <p>Load the first entry List
   *
   * <p>
   */
  private void loadLeafPage() {
    this.numKeys = this.byteBuffer.asIntBuffer().get(1);
    offset = 2;
    loadSingleDataEntryList();
  }

  /** After loaded the leaf page, load the next entry list from the leaf page */
  private void loadSingleDataEntryList() {
    while (true) {
      if (this.numKeys == 0) {
        loadPageByOffset(this.offset);
        return;
      }

      this.value = this.byteBuffer.asIntBuffer().get(offset++);
      this.count = this.byteBuffer.asIntBuffer().get(offset++);
      this.numKeys--;

      if (this.lowKey == Integer.MAX_VALUE && this.highKey == Integer.MAX_VALUE) {
        // no low key and high key
        break;
      } else if (this.highKey == Integer.MAX_VALUE) {
        // no high key
        if (this.value >= this.lowKey) {
          break;
        }
      } else if (this.lowKey == Integer.MAX_VALUE) {
        // no low key
        if (this.value <= this.highKey) {
          break;
        }
      } else if (this.value >= this.lowKey && this.value <= this.highKey) {
        break;
      }
      this.offset += this.count * 2;
    }
  }

  /**
   * Get the next tuple from the index file
   *
   * @return the next tuple
   */
  public Tuple next() {

    if (count == 0) {
      loadSingleDataEntryList();
    }
    count--;

    Pair<Integer, Integer> pair =
        new Pair<Integer, Integer>(
            this.byteBuffer.asIntBuffer().get(offset++),
            this.byteBuffer.asIntBuffer().get(offset++));

    // For non-Clustered index, retrieve tuple from the data file
    // For Clustered index, scan the sorted data file sequentially
    if (!this.isClustered || !this.isLoaded) {
      this.isLoaded = true;
      this.tupleReader.reset(pair.getLeft(), pair.getRight());
    }
    return this.tupleReader.readNextTuple();
  }
}
