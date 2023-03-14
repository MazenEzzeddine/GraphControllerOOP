package group;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;

public class ConsumerGroup {
    private static final Logger log = LogManager.getLogger(ConsumerGroup.class);

    String inputTopic;
    String name;
    String kafkaName;
    Integer size;
    ArrayList<Partition> topicpartitions;
    double totalArrivalRate;
    double totalLag;
    double dynamicAverageMaxConsumptionRate;
    double wsla = 5;
    Instant lastUpScaleDecision = Instant.now();


    public ConsumerGroup(String inputTopic, Integer size, double dynamicAverageMaxConsumptionRate, double wsla, String name, String kname) {
        this.inputTopic = inputTopic;
        this.size = size;
        this.dynamicAverageMaxConsumptionRate = dynamicAverageMaxConsumptionRate;
        this.wsla = wsla;
        this.name = name;
        this.kafkaName = kname;
        topicpartitions = new ArrayList<>();
        for (int i = 0; i <= 4; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
        }
    }

    public String getKafkaName() {
        return kafkaName;
    }

    public void setKafkaName(String kafkaName) {
        this.kafkaName = kafkaName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public double getDynamicAverageMaxConsumptionRate() {
        return dynamicAverageMaxConsumptionRate;
    }

    public void setDynamicAverageMaxConsumptionRate(double dynamicAverageMaxConsumptionRate) {
        this.dynamicAverageMaxConsumptionRate = dynamicAverageMaxConsumptionRate;
    }

    public double getWsla() {
        return wsla;
    }

    public void setWsla(double wsla) {
        this.wsla = wsla;
    }

    public Instant getLastUpScaleDecision() {
        return lastUpScaleDecision;
    }

    public void setLastUpScaleDecision(Instant lastUpScaleDecision) {
        this.lastUpScaleDecision = lastUpScaleDecision;
    }


    public String getInputTopic() {
        return inputTopic;
    }

    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    public ArrayList<Partition> getTopicpartitions() {
        return topicpartitions;
    }

    public void setTopicpartitions(ArrayList<Partition> topicpartitions) {
        this.topicpartitions = topicpartitions;
    }

    public double getTotalArrivalRate() {
        return totalArrivalRate;
    }

    public void setTotalArrivalRate(double totalArrivalRate) {
        this.totalArrivalRate = totalArrivalRate;
    }

    public double getTotalLag() {
        return totalLag;
    }

    public void setTotalLag(double totalLag) {
        this.totalLag = totalLag;
    }

}
