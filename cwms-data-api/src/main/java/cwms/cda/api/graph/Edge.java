package cwms.cda.api.graph;

public interface Edge {
    String getId();

    Node getSource();

    Node getTarget();
}
