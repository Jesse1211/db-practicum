package operator;

import common.ExpressionEvaluator;
import common.HelperMethods;
import common.Tuple;
import java.util.ArrayList;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.schema.Column;

// Build a query plan that is a tree of operators.
public class SelectOperator extends Operator {

  private ScanOperator scanOperator;
  private BinaryExpression whereExpression;
  private Map<String, Integer> columnIndexMap;

  public SelectOperator(ArrayList<Column> outputSchema, ScanOperator scanOperator, BinaryExpression whereExpression) {
    super(outputSchema);
    this.scanOperator = scanOperator;
    this.whereExpression = whereExpression;
    this.columnIndexMap = HelperMethods.mapColumnIndex(outputSchema);
  }

  @Override
  public void reset() {
    // for (Operator operator : child) {
    // operator.reset();
    // }
  }

  @Override
  public Tuple getNextTuple() {
    Tuple tuple;
    if ((tuple = scanOperator.getNextTuple()) != null) {
      ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, columnIndexMap);
      whereExpression.accept(evaluator);
      boolean result = evaluator.getResult();
      System.out.println(result);
      if (result){
        return tuple;
      }else{
        getNextTuple();
      }
    };
    return null;
  }
}
