import graph.Graph;
import graph.Vertex;
import group.ConsumerGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutionException;


public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        initialize();

    }



     private static void initialize() throws InterruptedException, ExecutionException {
         //System.out.println(topoOrder);
         Graph g = new Graph(3);
         g.addVertex(0, "testtopic1", "cons1persec");
         g.addVertex(1, "testtopic2", "cons1persec2");
         g.addVertex(2, "testtopic3", "cons1persec5");
         g.addEdge(0, 1);
         g.addEdge(1, 2);

         Stack<Vertex> ts = g.dfs(g.getVertex(0)); // 1 2 3 4 5
         List<Vertex> topoOrder = new ArrayList<>();
         // System.out.println(ts);
         while(!ts.isEmpty()) {
             topoOrder.add(ts.pop());
         }
         System.out.println(topoOrder);

         ConsumerGroup[] cgs = new ConsumerGroup[topoOrder.size()];
         for (int i = 0; i < topoOrder.size(); i++) {
             cgs[i] = new ConsumerGroup(topoOrder.get(i).getTopic(),2, 175, 1.6,
                     topoOrder.get(i).getConsumerGroupName());
        }




        log.info("Warming for 2 minutes seconds.");
        Thread.sleep(60*2*1000);
         //Thread.sleep(30);

         while (true) {
            log.info("Querying Prometheus");
            Main.QueryingPrometheus(cgs, g);
            log.info("Sleeping for 5 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(5000);
        }
    }


    static void QueryingPrometheus( ConsumerGroup[] cgs, Graph g) throws ExecutionException, InterruptedException {

        ArrivalRates.arrivalRateTopic1(cgs);
        ArrivalRates.arrivalRateTopic2(cgs[1]);
        ArrivalRates.arrivalRateTopic5(cgs[2]);

        Util.computeBranchingFactors(g.getAdjMat(), cgs);
        ArrivalRates.arrivalRateTopic1(cgs);

        if (Duration.between(cgs[0].getLastUpScaleDecision(), Instant.now()).getSeconds() > 15) {
            //QueryRate.queryConsumerGroup();
           BinPack.scaleAsPerBinPack(cgs[0]);
        }
        if (Duration.between(cgs[1].getLastUpScaleDecision(), Instant.now()).getSeconds() > 15) {
            //QueryRate.queryConsumerGroup();
            BinPack.scaleAsPerBinPack(cgs[1]);
        }
        if (Duration.between(cgs[2].getLastUpScaleDecision(), Instant.now()).getSeconds() > 15) {
           // QueryRate.queryConsumerGroup();
            BinPack.scaleAsPerBinPack(cgs[2]);
        }
    }





}
