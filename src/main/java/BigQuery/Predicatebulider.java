package BigQuery;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

public class Predicatebulider {
    Bigquery bq = new Bigquery();
    String handleCurrentDate(JSONArray arr)
    {
        return "ds";
    }

    String handlePred(JSONArray predicates) throws IOException {
        int betweenFlag=0;
        int inFlag=0;
        ExpressionBuilder eb = new ExpressionBuilder();
        String pred="";
        for (int i = 0; i < predicates.length(); i++) {

            if (betweenFlag == 1) {

                JSONObject operand1 = predicates.getJSONObject(i).getJSONObject("operand1");
                JSONObject operand2 = predicates.getJSONObject(i).getJSONObject("operand2");
                String op1 = eb.Expression(operand1.getJSONArray("expression"));
                String op2 = eb.Expression(operand2.getJSONArray("expression"));
                betweenFlag = 0;
                pred = pred +   " BETWEEN " + op1 + " AND " + op2;
                continue;
            }
            if (predicates.getJSONObject(i).has("expr_type") && predicates.getJSONObject(i).getString("expr_type").equals("operator")) {

                if (predicates.getJSONObject(i).getString("operator").equalsIgnoreCase("BETWEEN")) {
                    betweenFlag = 1;
                }
                else if(predicates.getJSONObject(i).getString("operator").equalsIgnoreCase("IN"))
                {
                    inFlag=1;
                }
                else {
                    pred = pred + " " + predicates.getJSONObject(i).getString("operator")+" ";
                }
            } else if (predicates.getJSONObject(i).has("nestedcondtnobj")) {
                JSONArray nestedarray = predicates.getJSONObject(i).getJSONArray("nestedcondtnobj");
                pred = pred + "(" + handlePred(nestedarray) + ")";
            }
            else if(inFlag==1)
            {
                String in=" IN (";
                String t="";
                if(predicates.getJSONObject(i).getString("type").equalsIgnoreCase("expressionentity"))
                {
                    JSONArray expressArray=predicates.getJSONObject(i).getJSONArray("expression");

                    for(int p=0;p<expressArray.length();p++)
                    {
                        if(p==expressArray.length()-1) {
                            t = t + eb.Expression(expressArray.getJSONArray(p));
                        }
                        else
                        {
                            t = t + eb.Expression(expressArray.getJSONArray(p)) + ",";
                        }
                    }

                }
                else if(predicates.getJSONObject(i).getString("type").equalsIgnoreCase("selectentity"))
                {

                    try {
                        t= "("+bq.objectParser(predicates.getJSONObject(i).getJSONArray("expression").get(0).toString())+")";
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                pred=pred+in+t+")";
                inFlag=0;

            }
            else {
                if (predicates.getJSONObject(i).has("type") && predicates.getJSONObject(i).getString("type").equals("selectentity")) {

//                String s=tableObject.getJSONObject("FROM");
//                System.out.println(tableObject.getJSONObject("def").getJSONObject("entity").toString());
                    JSONArray predExpression = predicates.getJSONObject(i).getJSONArray("expression");
                        pred = pred + " ( " + bq.objectParser(predicates.getJSONObject(i).getJSONArray("expression").get(0).toString()) + " ) ";
                } else {
                    JSONArray expression = predicates.getJSONObject(i).getJSONArray("expression");
                    pred = pred + eb.Expression(expression);
                }
            }
        }
        return pred;
    }
}
