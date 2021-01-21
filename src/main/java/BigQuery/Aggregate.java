package BigQuery;

import org.json.JSONArray;

import java.io.IOException;

public class Aggregate {

    public String buildAggregate(JSONArray expression) throws IOException {
        FunctionImpl function = new FunctionImpl();
        EntityImpl entity = new EntityImpl();
        LiteralImpl literal = new LiteralImpl();
        String res="";
        for (int j = 0; j < expression.length(); j++) {
            String stoadd="";
            ExpressionBuilder eb = new ExpressionBuilder();
          stoadd= eb.Expression(expression.getJSONArray(j));
            if(j==expression.length()-1)
                res=res+stoadd;
            else
                res=res+stoadd+",";
        }
        return res;
    }
}
