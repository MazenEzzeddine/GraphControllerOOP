import graph.Graph;
import group.ConsumerGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ArrivalRates {
    private static final Logger log = LogManager.getLogger(ArrivalRates.class);

    static void arrivalRateTopic2(ConsumerGroup g) {
        HttpClient client = HttpClient.newHttpClient();

        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic2p0),
                    new URI(Constants.topic2p1),
                    new URI(Constants.topic2p2),
                    new URI(Constants.topic2p3),
                    new URI(Constants.topic2p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic2p0lag),
                    new URI(Constants.topic2p1lag),
                    new URI(Constants.topic2p2lag),
                    new URI(Constants.topic2p3lag),
                    new URI(Constants.topic2p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            g.getTopicpartitions().get(partition2).setArrivalRate(partitionArrivalRate2);
            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        log.info("totalArrivalRate for  topic 2 {}", totalarrivalstopic2);
        g.setTotalArrivalRate(totalarrivalstopic2);
        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;
        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            g.getTopicpartitions().get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }
        g.setTotalLag(totallag2);
        log.info("totalLag for topic 2 {}", totallag2);
        log.info("******************");
    }


    static void arrivalRateTopic1(Graph g) {
        HttpClient client = HttpClient.newHttpClient();
        ////////////////////////////////////////////////////
        List<URI> partitions = new ArrayList<>();
        try {
            partitions = Arrays.asList(
                    new URI(Constants.topic1p0),
                    new URI(Constants.topic1p1),
                    new URI(Constants.topic1p2),
                    new URI(Constants.topic1p3),
                    new URI(Constants.topic1p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag = new ArrayList<>();
        try {
            partitionslag = Arrays.asList(
                    new URI(Constants.topic1p0lag),
                    new URI(Constants.topic1p1lag),
                    new URI(Constants.topic1p2lag),
                    new URI(Constants.topic1p3lag),
                    new URI(Constants.topic1p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////
        //launch queries for topic 1 lag and arrival get them from prometheus
        List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition = 0;
        double totalarrivalstopic1 = 0.0;
        double partitionArrivalRate = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures) {
            try {
                partitionArrivalRate = Util.parseJsonArrivalRate(cf.get(), partition);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            g.getVertex(0).getG().getTopicpartitions().get(partition).setArrivalRate(partitionArrivalRate);
            g.getVertex(1).getG().getTopicpartitions().get(partition).setArrivalRate(partitionArrivalRate);
            g.getVertex(2).getG().getTopicpartitions().get(partition).setArrivalRate(partitionArrivalRate);

           // Scale5p.topicpartitions5.get(partition).setArrivalRate(partitionArrivalRate*0.7);

            totalarrivalstopic1 += partitionArrivalRate;
            partition++;
        }
        log.info("totalArrivalRate for  topic 1 {}", totalarrivalstopic1);
        g.getVertex(0).getG().setTotalArrivalRate(totalarrivalstopic1);

        partition = 0;
        double totallag = 0.0;
        long partitionLag = 0L;
        for (CompletableFuture<String> cf : partitionslagfuture) {
            try {
                partitionLag = Util.parseJsonArrivalLag(cf.get(), partition).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            g.getVertex(0).getG().getTopicpartitions().get(partition).setLag(partitionLag);

            /*Scale2.topicpartitions2.get(partition).setLag((long)(partitionLag*0.7));
            Scale5.topicpartitions5.get(partition).setLag((long)(partitionLag*0.7));*/
             /* Scale2p.topicpartitions2.get(partition).setLag(0);
            Scale5p.topicpartitions5.get(partition).setLag(0);*/


            totallag += partitionLag;
            partition++;
        }


        g.getVertex(0).getG().setTotalLag(totallag);

        double noise = Math.max(totalarrivalstopic1, g.getVertex(0).getG().getSize()*175 );
        double actualLag = Math.max(0, totallag - noise);



        log.info("totalLag for topic 1 {}", totallag);
        log.info("total actuaLag for topic 1 {}", actualLag);

/*        for (int i = 0; i <= 4; i++) {
            *//*Scale2.topicpartitions2.get(i).setArrivalRate(Scale2.topicpartitions2.get(i).getArrivalRate() + ((actualLag*0.7) / (5*1.66)));
            Scale5.topicpartitions5.get(i).setArrivalRate(Scale5.topicpartitions5.get(i).getArrivalRate() + ((actualLag*0.7)/ (5*1.666)));
            Scale1.topicpartitions1.get(i).setLag((long)(actualLag / (5)));


            /*Scale2.topicpartitions2.get(i).setArrivalRate(Scale2.topicpartitions2.get(i).getArrivalRate() + ((actualLag*0.7) / (5*1.66)));
            Scale5.topicpartitions5.get(i).setArrivalRate(Scale5.topicpartitions5.get(i).getArrivalRate() + ((actualLag*0.7)/ (5*1.666)));
            Scale1.topicpartitions1.get(i).setLag((long)(actualLag / (5)));
*//*

            log.info( "Scale2.topicpartitions2.get(i) {} , {} ", i, Scale2.topicpartitions2.get(i).getArrivalRate());
            log.info( "Scale5.topicpartitions5.get(i) {} , {} ", i, Scale5.topicpartitions5.get(i).getArrivalRate());
            log.info( " Scale1.topicpartitions1.get(i).setLag  {} , {}", i, Scale1.topicpartitions1.get(i).getLag());


        }*/



        log.info("Enf of arrival rates computations");

        log.info("=========================================");
    }





    static void arrivalRateTopic5(ConsumerGroup g) {
        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic5p0),
                    new URI(Constants.topic5p1),
                    new URI(Constants.topic5p2),
                    new URI(Constants.topic5p3),
                    new URI(Constants.topic5p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic5p0lag),
                    new URI(Constants.topic5p1lag),
                    new URI(Constants.topic5p2lag),
                    new URI(Constants.topic5p3lag),
                    new URI(Constants.topic5p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        /////////////////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            g.getTopicpartitions().get(partition2).setArrivalRate(partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        g.setTotalArrivalRate(totalarrivalstopic2);
        log.info("totalArrivalRate for  topic 5 {}", totalarrivalstopic2);


        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;

        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            g.getTopicpartitions().get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }

        log.info("totalLag for topic 5 {}", totallag2);
     /*   for (int i = 0; i <= 4; i++) {
            log.info("topic 5 partition {} has the following arrival rate {} and lag {}", i, Scale5p.topicpartitions5.get(i).getArrivalRate(),
                    Scale5p.topicpartitions5.get(i).getLag());
        }*/
        g.setTotalLag(totallag2);

        log.info("******************");

    }





}
