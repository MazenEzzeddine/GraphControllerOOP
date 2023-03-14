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

         ConsumerGroup g0 = new ConsumerGroup("testtopic1", 2, 175, 5, "cons1persec", "testgroup1");
         ConsumerGroup g1 = new ConsumerGroup("testtopic2", 2,  175, 5, "cons1persec2", "testgroup2");
         ConsumerGroup g2 = new ConsumerGroup("testtopic5", 2,  175, 5, "cons1persec5", "testgroup5");


         g.addVertex(0, g0);
         g.addVertex(1, g1);
         g.addVertex(2, g2);
         g.addEdge(0, 1);
         g.addEdge(1, 2);



         Stack<Vertex> ts = g.dfs(g.getVertex(0)); // 1 2 3 4 5
         List<Vertex> topoOrder = new ArrayList<>();
         // System.out.println(ts);
         while(!ts.isEmpty()) {
             topoOrder.add(ts.pop());
         }










/*
        log.info("Warming for 2 minutes seconds.");
        Thread.sleep(60*2*1000);*/


         log.info("Warming 30  seconds.");
         Thread.sleep(30*1000);

         //Thread.sleep(30);

         while (true) {
            log.info("Querying Prometheus");
            Main.QueryingPrometheus(g, topoOrder);
            log.info("Sleeping for 5 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(5000);
        }
    }


    static void QueryingPrometheus( Graph g, List<Vertex> topoOrder) throws ExecutionException, InterruptedException {



        // cpmpute arrival rates before autoscaling to get the BFs
        for (int i = 0; i < topoOrder.size(); i++) {
            ArrivalRates.arrivalRateTopicGeneral( g.getVertex(i).getG());
        }



       /* ArrivalRates.arrivalRateTopic1(g);
        ArrivalRates.arrivalRateTopic2(g.getVertex(1).getG());
        ArrivalRates.arrivalRateTopic2(g.getVertex(2).getG());*/



        Util.computeBranchingFactors(g);
        ArrivalRates.arrivalRateTopic1(g);

        if (Duration.between(g.getVertex(0).getG().getLastUpScaleDecision(), Instant.now()).getSeconds() > 15) {
            //QueryRate.queryConsumerGroup();
           BinPack.scaleAsPerBinPack(g.getVertex(0).getG());
        }
        if (Duration.between(g.getVertex(1).getG().getLastUpScaleDecision(), Instant.now()).getSeconds() > 15) {
            //QueryRate.queryConsumerGroup();
            BinPack.scaleAsPerBinPack(g.getVertex(1).getG());
        }
        if (Duration.between(g.getVertex(2).getG().getLastUpScaleDecision(), Instant.now()).getSeconds() > 15) {
           // QueryRate.queryConsumerGroup();
            BinPack.scaleAsPerBinPack(g.getVertex(2).getG());
        }
    }





}
