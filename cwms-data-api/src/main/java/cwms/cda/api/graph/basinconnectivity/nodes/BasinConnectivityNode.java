package cwms.cda.api.graph.basinconnectivity.nodes;

import cwms.cda.api.graph.Node;
import cwms.cda.api.graph.basinconnectivity.BasinConnectivityStream;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public abstract class BasinConnectivityNode implements Node {
    private final String streamId;
    private final Double station;
    private final String bank;

    protected BasinConnectivityNode(String streamId, Double station, String bank) {
        this.streamId = streamId;
        this.station = station;
        if (bank == null) {
            bank = "L"; //default bank
        }
        this.bank = bank;
    }

    public String getBank() {
        return bank;
    }

    public Double getStation() {
        return station;
    }

    public String getStreamId() {
        return streamId;
    }

    public abstract String getLabel();

    @Override
    public boolean equals(Object obj) {
        boolean retval = false;
        if (obj instanceof BasinConnectivityNode) {
            BasinConnectivityNode other = (BasinConnectivityNode) obj;
            retval = (other.getId() != null
                    && other.getStreamId() != null
                    && other.getStation() != null);
            if (retval) {
                retval = other.getId().equalsIgnoreCase(getId())
                        && other.getStation().equals(getStation())
                        && other.getStreamId().equalsIgnoreCase(getStreamId());
            }
        }
        return retval;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        String name = getId();
        result = prime * result + ((name == null) ? 0 : name.toLowerCase().hashCode());
        result = prime * result + ((station == null) ? 0 : station.hashCode());
        result = prime * result + ((streamId == null) ? 0 : streamId.toLowerCase().hashCode());
        return result;
    }

    public static BasinConnectivityNode getNode(BasinConnectivityStream parentStream,
                                                @NotNull String name) {
        BasinConnectivityNode retval = null;
        List<BasinConnectivityNode> streamNodes = parentStream.getStreamNodes();
        for (BasinConnectivityNode node : streamNodes) {
            if (name.equalsIgnoreCase(node.getId())) {
                retval = node;
                break;
            }
        }
        return retval;
    }
}
