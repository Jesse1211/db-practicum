package operator;

import common.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import java.util.*;

/*
 * Operator for the Project operation: SELECT *Sailors.id* FROM Sailors WHERE Sailors.age = 20
 * [Assume queries do not use aliases]
 */
public class ProjectOperator extends Operator {
    private Operator child;
    private List<SelectItem> selectedItems;
    private Map<String, Integer> columnMap = new HashMap<>(); // column name : index

    public ProjectOperator(ArrayList<Column> outputSchema, Operator child, List<SelectItem> selectedItems) {
        super(outputSchema);
        this.child = child;
        this.selectedItems = selectedItems;

        // Create a map of column name to index in the output schema
        for (int i = 0; i < outputSchema.size(); i++) {
            columnMap.put(outputSchema.get(i).getColumnName(), i);
        }
    }

    @Override
    public void reset() {

    }

    @Override
    public Tuple getNextTuple() {
        Tuple childTuple = child.getNextTuple();

        if (childTuple != null) {
            ArrayList<Integer> tupleArray = new ArrayList<>();

            for (SelectItem item : selectedItems) {
                if (item instanceof SelectExpressionItem) {
                    Column column = (Column) ((SelectExpressionItem) item).getExpression();
                    String columnName = column.getColumnName();
                    // Find the index of the column in the output schema
                    int columnIndex = columnMap.get(columnName);

                    // Add the value from the child tuple's data to the new tuple's data array
                    tupleArray.add(childTuple.getElementAtIndex(columnIndex));
                }

            }
            return new Tuple(tupleArray);
        }
        return null;
    }
}
