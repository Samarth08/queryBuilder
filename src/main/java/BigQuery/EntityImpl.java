package BigQuery;

import org.json.JSONObject;

public class EntityImpl {
    public String entityBuilder(JSONObject jsonObject) {
        String colName = jsonObject.get("colname").toString();
        String entityAlias = "";
        String colAlias = "";
        try {
            entityAlias = jsonObject.get("enityalias").toString();
        } catch (Exception e) {
            entityAlias = "";
        }

        try {
            colAlias = jsonObject.get("colalias").toString();
        } catch (Exception e) {
            colAlias = "";
        }
        String str = "";
        if (entityAlias.equals("")) {
            str = str + colName;
        } else {
            str = str + entityAlias + "." + colName;
        }
        if (colAlias.equals("")) {
            return str;

        } else {
            return str + " AS " + colAlias;
        }

    }
}
