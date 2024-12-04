package common;

import common.pair.Pair;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

// R.A < 100
public class ComparisonEvaluator {
  private Set<ComparisonOperator> residuals= new HashSet<>();
  private Map<String, ComparisonOperator> notEqualToValueMap = new HashMap<>();
  private Set<Pair<String, String>> equalityJoinMap = new HashSet<>();
  private UnionFind unionFind = UnionFind.getInstance(true);

  public void visit(ComparisonOperator comparison){
    Expression leftExpression = comparison.getLeftExpression();
    Expression rightExpression = comparison.getRightExpression();

    if (leftExpression instanceof LongValue && rightExpression instanceof LongValue) {
      residuals.add(comparison);
      return;
    }

    // if join condition R.A = S.B, 2 columns, then union them.
    if (leftExpression instanceof Column && rightExpression instanceof Column) {
      if (comparison instanceof EqualsTo) {
        Column left = (Column) leftExpression;
        Column right = (Column) rightExpression;
        UnionFindElement e1 = unionFind.find(left);
        UnionFindElement e2 = unionFind.find(right);
        unionFind.union(e1, e2);
        equalityJoinMap.add(new Pair<>(left.getName(true), right.getName(true)));
      }
      residuals.add(comparison);
      return;
    }

    if (comparison instanceof NotEqualsTo) {
      Pair<String, String> names = HelperMethods.getComparisonTableNames(comparison);
      String name = names.getLeft() != null ? names.getLeft() : names.getRight();
      notEqualToValueMap.put(name, comparison);
    }
    // else R.A = 8, set the bound.
    Column attribute = (leftExpression instanceof Column) ? (Column)leftExpression : (Column)rightExpression;
    Pair<Integer, Integer> pair = HelperMethods.evaluateComparison(comparison); // low, high
    UnionFindElement e = unionFind.find(attribute);
    unionFind.addBounds(e, pair.getLeft(), pair.getRight());
  }

  public Set<UnionFindElement> getResult(){
    return unionFind.getElements();
  }

  public Set<ComparisonOperator> getResiduals(){
    return this.residuals;
  }

  public Map<String, ComparisonOperator> getNotEqualToValueMap(){
    return this.notEqualToValueMap;
  }

  public Set<Pair<String, String>> getEqualityJoinMap(){return this.equalityJoinMap;}
}
