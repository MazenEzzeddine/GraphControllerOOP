package graph;

import group.ConsumerGroup;

public class Vertex {
    int label;
    String topic;

    public ConsumerGroup getG() {
        return g;
    }

    public void setG(ConsumerGroup g) {
        this.g = g;
    }

    ConsumerGroup g;


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


    public boolean isVisited() {
        return isVisited;
    }

    public void setVisited(boolean visited) {
        isVisited = visited;
    }

    boolean isVisited;

    Vertex(int label,  ConsumerGroup g) {
        this.label = label;
        isVisited = false;
        this.g = g;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "label=" + label +
                ", topic='" + topic + '\'' +
                '}' + "\n";
    }
}
