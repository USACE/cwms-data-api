package cwms.radar.api.graph.basinconnectivity.nodes;

import cwms.radar.data.dto.basinconnectivity.StreamLocation;

public class BasinConnectivityStreamLocation extends BasinConnectivityNode {

    private static final String LABEL = "Stream Location";
    private final StreamLocation streamLocation;

    public BasinConnectivityStreamLocation(StreamLocation streamLocation) {
        super(streamLocation.getStreamName(), streamLocation.getStation(),
                streamLocation.getBank());
        this.streamLocation = streamLocation;
    }

    @Override
    public String getId() {
        return streamLocation.getLocationName();
    }

    @Override
    public String getLabel() {
        return LABEL;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
