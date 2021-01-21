package BigQuery;

import org.json.JSONObject;

public class LiteralImpl {
    public String literalBuilder(JSONObject jsonObject) {
        String alias = "";
        String val = jsonObject.get("literalvalue").toString();
        try {
            alias = jsonObject.get("colalias").toString();
        } catch (Exception e) {
            alias = "";
        }
        String res = "";
        if (alias.equals("")) {
            return val;
        } else {
            return val + " AS " + alias;
        }
    }
}
