import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import graph.Graph;
import group.ConsumerGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Util {


    private static final Logger log = LogManager.getLogger(Util.class);


    static Double parseJsonArrivalRate(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        return Double.parseDouble(jreq.getString(1));
    }


    static Double parseJsonArrivalLag(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        return Double.parseDouble(jreq.getString(1));
    }


    static void computeBranchingFactors (Graph g) {
        int [][] A = g.getAdjMat();
        for (int m = 0; m < A.length; m++) {
            double parentsArrivalRate= 0;
            boolean issource= true;
            for (int parent = 0; parent < A[m].length; parent++) {
                if (A[parent][m] == 1) {
                    //log.info( " {} {} is a prarent of {} {}", parent, g.getVertex(parent).getG() , m, g.getVertex(m).getG() );
                    parentsArrivalRate += Math.min(g.getVertex(parent).getG().getTotalArrivalRate(),
                            g.getVertex(parent).getG().getSize()* g.getVertex(parent).getG().getDynamicAverageMaxConsumptionRate());
                    issource = false;
                }
            }

            if (issource) {
                g.getVertex(m).getG().setBranchFactor(1.0);

            } else {
                g.getVertex(m).getG().setBranchFactor(g.getVertex(m).getG().getTotalArrivalRate()/parentsArrivalRate);
            }
        }
    }
}
