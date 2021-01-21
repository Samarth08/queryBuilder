package BigQuery;

import org.json.JSONObject;

import java.io.IOException;

public class TableMeta {

    public String tableParser(JSONObject jsonObject)
    {
        String schemaname="";
        try{
            schemaname=jsonObject.getString("schemaname");
        }
        catch (Exception e)
        {

        }
        if(schemaname.equals(""))
        {
            return jsonObject.getString("tablename");
        }
        else
        {
            return jsonObject.getString("schemaname")+"."+jsonObject.getString("tablename");
        }
    }

    public String OnParser(JSONObject jsonObject) throws IOException {
        ExpressionBuilder expressionBuilder = new ExpressionBuilder();
        return expressionBuilder.FromExpress(jsonObject.getJSONArray("ON"));
    }
}
