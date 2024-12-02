package common.index;

import common.pair.Pair;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to contain information about index: relation name, attribute name, whether it is clustered
 * and order of the index.
 */
public class IndexInfo {
  public String relationName;
  // change to map
  public Map<String, Pair<Boolean, Integer>> attributes = new HashMap<>();
//  public String attributeName;
//  public boolean isClustered;
//  public int order;

  /**
   * Constructor for IndexInfo
   *
   * @param relationName relation name
   */
  public IndexInfo(String relationName) {
    this.relationName = relationName;
  }

  /**
   * Add an attribute to IndexInfo
   *
   * @param attributeName
   * @param isClustered
   * @param order
   */
  public void addAttribute(String attributeName, boolean isClustered, int order) {
    attributes.put(attributeName, new Pair<>(isClustered, order));
  }
}
