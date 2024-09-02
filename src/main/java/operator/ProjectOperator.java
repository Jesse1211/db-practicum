package operator;

import common.HelperMethods;
import common.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import java.util.*;

/*
 * Operator for the Project operation: SELECT *Sailors.id* FROM Sailors WHERE Sailors.age = 20
 * [Assume queries do not use aliases]
 */
public class ProjectOperator extends Operator {
    private Operator childOperator;
    private List<SelectItem> selectItems;
    private Map<String, Integer> columnIndexMap = new HashMap<>(); // column name : index

    public ProjectOperator(ArrayList<Column> outputSchema, Operator childOperator, List<SelectItem> selectItems) {
        super(outputSchema);
        this.childOperator = childOperator;
        this.selectItems = selectItems;
        this.columnIndexMap = HelperMethods.mapColumnIndex(outputSchema);
        updateOutputSchema();
    }

    @Override
    public void reset() {

    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;

        if ((tuple = childOperator.getNextTuple()) != null) {
            ArrayList<Integer> tupleArray = new ArrayList<>();

            for (Column column : outputSchema) {
                // Find the index of the column in the output schema
                int index = columnIndexMap.get(column.getName(false));

                // Add the value from the child tuple's data to the new tuple's data array
                tupleArray.add(tuple.getElementAtIndex(index));
            }
            return new Tuple(tupleArray);
        }
        return null;
    }


    private void updateOutputSchema() {
        ArrayList<Column> outputSchema = new ArrayList<>();
        for (SelectItem item : selectItems) {
            if (item instanceof SelectExpressionItem) {
                Column column = (Column) ((SelectExpressionItem) item).getExpression();
                int index = columnIndexMap.get(column.getName(false));
                outputSchema.add(this.outputSchema.get(index));
            }else if(item instanceof AllColumns){
                outputSchema.addAll(this.outputSchema);
            }
        }
        this.outputSchema = outputSchema;
    }
}
