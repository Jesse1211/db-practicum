package common;

import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class HelperMethods {
  public static Map<String, Integer> mapColumnIndex(ArrayList<Column> columns) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (int i = 0; i < columns.size(); i++) {
      map.put(columns.get(i).getName(false), i);
    }
    return map;
  }

  public static void sth(Expression expression) {
    ArrayList<Column> columns = new ArrayList<Column>();
    Map<String, List<Expression>> map = new HashMap<String, List<Expression>>();
    Queue<Expression> queue = new LinkedList<Expression>();
    queue.add(expression);

    while (!queue.isEmpty()) {
      // todo: 这里需要考虑到优先级

      // map => table name : list of expressions
      // AND OR => 还需要考虑到优先级
      Expression cur = queue.poll();
    }

    System.out.println(expression);
  }
}
