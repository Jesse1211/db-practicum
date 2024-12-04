package common;

import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;

public class UnionFindElement {
  public HashMap<String, Column> attributes = new HashMap<>();
  public int lowerBound = Integer.MIN_VALUE;
  public int upperBound = Integer.MAX_VALUE;
}
