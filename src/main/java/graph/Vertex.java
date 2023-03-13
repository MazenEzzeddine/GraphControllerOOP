package graph;

public class Vertex {
    int label;
    String topic;
    String consumerGroupName;


    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }


    public boolean isVisited() {
        return isVisited;
    }

    public void setVisited(boolean visited) {
        isVisited = visited;
    }

    boolean isVisited;

    Vertex(int label, String t, String cgn) {
        this.label = label;
        isVisited = false;
        topic = t;
        consumerGroupName =cgn;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "label=" + label +
                ", topic='" + topic + '\'' +
                ", consumerGroupName='" + consumerGroupName + '\'' +
                '}' + "\n";
    }
}
