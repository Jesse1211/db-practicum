package common;

import java.util.HashSet;
import java.util.Set;
import net.sf.jsqlparser.schema.Column;

public class UnionFind {
  private static UnionFind unionFind;
  private Set<UnionFindElement> elements = new HashSet<>();

  public static UnionFind getInstance(boolean reset) {
    if (unionFind == null || reset) {
      unionFind = new UnionFind();
    }
    return unionFind;
  }

  public UnionFindElement find(Column attribute) {
    // given a particular attribute, find and return the union-find element
    // containing that attribute;
    // if no such element is found, create it and return it.
    String columnName = attribute.getName(true);
    for(UnionFindElement e: elements){
      if(e.attributes.containsKey(columnName)){
        return e;
      }
    }

    UnionFindElement e = new UnionFindElement();
    e.attributes.put(columnName, attribute);
    elements.add(e);
    return e;
  }

  public void union(UnionFindElement e1, UnionFindElement e2){
    // merge smaller rank into larger rank
    if (e1.attributes.size() > e2.attributes.size()) {
      e1.attributes.putAll(e2.attributes);
      e1.lowerBound = Math.max(e1.lowerBound, e2.lowerBound);
      e1.upperBound = Math.min(e1.upperBound, e2.upperBound);
      elements.remove(e2);
    }else{
      e2.attributes.putAll(e1.attributes);
      e2.lowerBound = Math.max(e2.lowerBound, e1.lowerBound);
      e2.upperBound = Math.min(e2.upperBound, e1.upperBound);
      elements.remove(e1);
    }
  }

  public void addBounds(UnionFindElement e, int lowerBound, int upperBound){
    e.lowerBound = Math.max(e.lowerBound, lowerBound);
    e.upperBound = Math.min(e.upperBound, upperBound);
  }

  public Set<UnionFindElement> getElements(){
    return elements;
  }
}



