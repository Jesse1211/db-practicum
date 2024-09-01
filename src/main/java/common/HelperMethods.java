package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;

public class HelperMethods {
    public static Map<String, Integer> mapColumnIndex(ArrayList<Column> columns){
        Map<String, Integer> map = new HashMap<String, Integer>();
        for(int i = 0; i < columns.size(); i++) {
            map.put(columns.get(i).getName(false), i);
        }
        return map;
    }
}
