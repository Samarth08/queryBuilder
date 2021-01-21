package BigQuery;

import org.json.JSONArray;

import java.io.IOException;

public class ExpressionBuilder {
    public String Expression(JSONArray expression) throws IOException {
        String stoadd = "";
        StringBuilder express = new StringBuilder();
        FunctionImpl function = new FunctionImpl();
        EntityImpl entity = new EntityImpl();
        LiteralImpl literal = new LiteralImpl();
        Bigquery bq= new Bigquery();
        ExpressionBuilder eb= new ExpressionBuilder();
        int flag=0;
        for (int j = 0; j < expression.length(); j++) {
            if(expression.getJSONObject(j).getString("expr_type").equals("operand")&&expression.getJSONObject(j).getString("type").equals("case_when")) {
                JSONArray then;
                JSONArray when;
                JSONArray esel;
                JSONArray casearray = expression.getJSONObject(j).getJSONArray("def");
                String thenpart = "";
                String whenpart = "";
                String elsepart="";
                String finalpart="";
                String s= " CASE ";
                String res="";
                for (int l = 0; l < casearray.length(); l++) {
                    int len = expression.getJSONObject(j).getJSONArray("def").getJSONObject(l).length();
                    if (len == 2) {
                        then = expression.getJSONObject(j).getJSONArray("def").getJSONObject(l).getJSONArray("then");
                        thenpart = thenpart + Expression(then);
                        thenpart = " then " + thenpart;

                        when = expression.getJSONObject(j).getJSONArray("def").getJSONObject(l).getJSONArray("when");
                        Predicatebulider pb = new Predicatebulider();
                        whenpart = whenpart + pb.handlePred(when);
                        whenpart = " when " + whenpart;

                    }
                else {
                        esel = expression.getJSONObject(j).getJSONArray("def").getJSONObject(l).getJSONArray("else");
                            elsepart=elsepart+Expression(esel);
                        elsepart=" else "+elsepart;
                        }

                    finalpart=" "+whenpart +thenpart+elsepart;
                    elsepart="";
                    thenpart="";
                    whenpart="";
                    s=s+finalpart;
                    }

                stoadd=stoadd+s+" END";
            }
            else if (expression.getJSONObject(j).getString("expr_type").equals("operator")) {
                stoadd = expression.getJSONObject(j).getString("operator");
                stoadd=" "+ stoadd+ " ";
            }
            else {
                String type = expression.getJSONObject(j).getString("type");
                if(flag==1)
                {

                }
                else if (type.equals("function")) {
                    stoadd = function.parser(expression.getJSONObject(j).getJSONObject("def"));
                } else if (type.equals("entity")) {
                    stoadd = entity.entityBuilder(expression.getJSONObject(j).getJSONObject("def"));
                } else if (type.equals("literal")) {
                    stoadd = literal.literalBuilder(expression.getJSONObject(j).getJSONObject("def"));
                }
                else if(type.equals("expression"))
                {
                    stoadd=eb.Expression(expression.getJSONObject(j).getJSONArray("def"));
                }
                else if(type.equals("keyword"))
                {
                    if(expression.getJSONObject(j).getJSONObject("def").getString("key").equals("CURRENT_DATE"))
                    {
                        stoadd="CURRENT_DATE";
                    }
                }
                else if(type.equals("extractfunc"))
                {
                    String tmpfun=eb.Expression(expression.getJSONObject(j).getJSONObject("def").getJSONArray("dateexpression"));
                    stoadd="Extract("+expression.getJSONObject(j).getJSONObject("def").getString("datekeyword")+" from "+tmpfun+")";

                }
                else if(type.equals("selectentity"))
                {
                        stoadd="("+bq.objectParser(expression.getJSONObject(j).get("def").toString())+")";

                }
            }
            String additional="";

            if(expression.getJSONObject(j).has("namedalias"))
            {
               additional=" "+expression.getJSONObject(j).getString("namedalias")+" ";
            }
            if(expression.getJSONObject(j).has("format"))
            {
                 additional=" "+" FORMAT "+additional+expression.getJSONObject(j).getString("format")+" ";
            }
            if(expression.getJSONObject(j).has("castdatatype"))
            {
                additional=" "+additional+expression.getJSONObject(j).getString("castdatatype")+" ";
            }
            express.append(stoadd);
            express.append(additional);
        }
        return express.toString();
    }
    public String FromExpress(JSONArray expression) throws IOException {
        String stoadd = "";
        StringBuilder express = new StringBuilder();
        for (int j = 0; j < expression.length(); j++) {
            if (expression.getJSONObject(j).getString("expr_type").equals("operator")) {
                stoadd = expression.getJSONObject(j).getString("operator");
                stoadd=" "+stoadd+" ";
            } else {
                    stoadd = Expression(expression.getJSONObject(j).getJSONArray("expression"));
            }
            express=express.append(stoadd);
        }
        return express.toString();
    }
}
