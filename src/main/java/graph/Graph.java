package graph;

import java.util.Stack;

public class Graph {
    private final Vertex[] V;
    private int vMax;

    public int[][] getAdjMat() {
        return adjMat;
    }

    private final int[][] adjMat;
    public int nV;
    private final Stack<Vertex> s;
    private final Stack<Vertex> topoStack;

    public Graph(int vMax) {
        this.vMax = vMax; // Maximum vertex can vbe added
        nV = 0; // counter for the vertices we will work with 1
        V = new Vertex[vMax ];
        adjMat = new int[vMax ][vMax];
        s = new Stack<>();
        topoStack = new Stack<>();
    }

    public void addVertex(int label, String topic, String cg) {
        V[nV] = new Vertex(label, topic, cg);
        nV++;
    }

    public void addEdge(int s, int d) {
        adjMat[s][d] = 1;
        //adjMat[d][s] = 1;
    }

    public Vertex getVertex(int i) {
        return V[i];
    }

    public Vertex unVisitedAdjVet(Vertex v) {
        for(int i=1; i<nV; i++) {
            if( adjMat[v.label][i] == 1 && !V[i].isVisited )
                return V[i];
        }
        return null;
    }

    public Stack<Vertex> dfs(Vertex start) {
        s.push(start);
        start.isVisited = true;
        //System.out.print(start);

        while(!s.isEmpty()) {
            Vertex vet = unVisitedAdjVet(s.peek());
                if (vet != null) {
                    vet.isVisited = true;
                   // System.out.print(vet);
                    s.push(vet);
                } else {
                   topoStack.push( s.pop());
                }
            }
        return  topoStack;
    }
}
// end class graph.Graph