package BigQuery;

import java.util.HashMap;
import java.util.Map;

public class Mapping {
    Map<String, String> data = new HashMap<String, String>();
    HashMap map = new HashMap();

    public String getBigQueryKeywordFrom(String teraFunc)
    {
        if(teraFunc.startsWith("char"))
        {
            teraFunc="char";
        }
        map.put("char","String");
        map.put("DECIMAL","NUMERIC");
        map.put("BITAND","BIT_AND");
        map.put("BITNOT","~");
        map.put("ADD_MONTHS","DATE_ADD");
        map.put("TO_DATE","PARSE_DATE");
        map.put("SUBSTRING","SUBSTR");
        map.put("RANDOM","RAND");
        map.put("ZEROIFNULL","IFNULL");
        map.put("NULLIFZERO","NULLIF");


        String bqfun="";
        try {
            bqfun=map.get(teraFunc).toString();
        }
        catch(Exception ex)
        {
            bqfun=teraFunc;
        }
        return bqfun;
    }
}

