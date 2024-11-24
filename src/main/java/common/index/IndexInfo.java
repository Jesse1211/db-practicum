package common.index;

/**
 * Class to contain information about index: relation name, attribute name, whether it is clustered
 * and order of the index.
 */
public class IndexInfo {
  public String relationName;
  public String attributeName;
  public boolean isClustered;
  public int order;

  /**
   * Constructor for IndexInfo
   *
   * @param relationName relation name
   * @param attributeName attribute name
   * @param isClustered whether it is clustered
   * @param order order of the index
   */
  public IndexInfo(String relationName, String attributeName, boolean isClustered, int order) {
    this.relationName = relationName;
    this.attributeName = attributeName;
    this.isClustered = isClustered;
    this.order = order;
  }
}
