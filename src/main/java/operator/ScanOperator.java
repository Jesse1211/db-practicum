package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;

public class ScanOperator extends Operator {
    private BufferedReader br;
    public ScanOperator(ArrayList<Column> outputSchema, String tableName) {
        super(outputSchema);
        try {
            File file = DBCatalog.getInstance().getFileForTable(tableName);
            br = new BufferedReader(new FileReader(file));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void reset() {
        try {
            br.reset();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public Tuple getNextTuple() {
        try {
            String line;
            if ((line = br.readLine()) != null) {
                return new Tuple(line);
            } else {
                br.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }
}
