package common;

import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;

/*
 * A UnionFindElement is a class that represents a set of attributes that are
 */
public class UnionFindElement {
  public HashMap<String, Column> attributes = new HashMap<>(); // Map of column name to column
  public int lowerBound = Integer.MIN_VALUE;
  public int upperBound = Integer.MAX_VALUE;
}
