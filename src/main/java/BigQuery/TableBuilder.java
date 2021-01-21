package BigQuery;

import org.json.JSONObject;

import java.io.IOException;

public class TableBuilder {

    public String TableBuilder(JSONObject tableObject) throws IOException {
        TableMeta tableMeta = new TableMeta();
        String drivingTable="";
        String from="";
        String on="";
        String key="";
        String val="";
        try {
            drivingTable=tableObject.getString("type");
        }
        catch (Exception ex)
        {
            drivingTable="";
        }
        if(drivingTable.equals("drivingtable"))
        {
            String tableAlias="";
            try {
                tableAlias=tableObject.getJSONObject("def").getString("tablealias");
            }
            catch (Exception ex)
            {
                tableAlias="";
            }
            if(tableObject.getJSONObject("def").getString("type").equals("tableentity"))
            {
            val= tableMeta.tableParser(tableObject.getJSONObject("def").getJSONObject("entity").getJSONObject("meta"));
            if(tableAlias.equals(""))
            {
                val=val;
            }
            else
            {
                val=val+" AS "+tableAlias;
            }
            }
            else if(tableObject.getJSONObject("def").getString("type").equals("selectentity"))
            {
                Bigquery bq=new Bigquery();
//                String s=tableObject.getJSONObject("FROM");

               String jsonArrayFrom=tableObject.getJSONObject("def").getJSONArray("entity").toString();
                val=val+ " ( "+bq.objectParser(jsonArrayFrom)+" ) ";

                if(tableObject.getJSONObject("def").has("tablealias"))
                {
                    String alias=tableObject.getJSONObject("def").getString("tablealias");
                    val=val+" AS "+alias;
                }

            }
        }
        else
        {
            String tableAlias="";
            try {
                tableAlias=tableObject.getJSONObject("FROM").getString("tablealias");
            }
            catch (Exception ex)
            {
                tableAlias="";
            }
            if(tableObject.getJSONObject("FROM").getString("type").equals("tableentity"))
            {
                from= tableMeta.tableParser(tableObject.getJSONObject("FROM").getJSONObject("entity").getJSONObject("meta"));
                if(tableAlias.equals(""))
                {
                }
                else
                {
                    from=from+" AS "+tableAlias;
                }
            }
            else if(tableObject.getJSONObject("FROM").getString("type").equals("selectentity"))
            {
                Bigquery bq=new Bigquery();
//                String s=tableObject.getJSONObject("FROM");
//                System.out.println(tableObject.getJSONObject("def").getJSONObject("entity").toString());
                from= "("+bq.objectParser(tableObject.getJSONObject("FROM").getJSONArray("entity").toString())+")";
                if(tableObject.getJSONObject("FROM").has("tablealias")) {
                    String alias = tableObject.getJSONObject("FROM").getString("tablealias");
                    from = from + " AS " + alias;
                }
                else
                {
                    from=from;
                }
            }
//            if(tableAlias.equals(""))
//            {
//                from=from;
//            }
//            else
//            {
//                from=from+" AS "+tableAlias;
//            }
            if(tableObject.has("ON")) {
                on = tableMeta.OnParser(tableObject);
                key = tableObject.getJSONObject("KEY").getString("joinkey");
                val = " " + key + " " + from + " " + " ON " + on + " ";
            }
            else if(tableObject.has("key"))
            {
                key = tableObject.getJSONObject("KEY").getString("joinkey");
                val = " " + key + " " + from;

            }
            else
            {
                val=from;
            }

        }
    return val;
    }
}
