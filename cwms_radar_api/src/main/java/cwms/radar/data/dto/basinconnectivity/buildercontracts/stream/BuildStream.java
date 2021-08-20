package cwms.radar.data.dto.basinconnectivity.buildercontracts.stream;

import cwms.radar.data.dto.basinconnectivity.Stream;
import cwms.radar.data.dto.basinconnectivity.StreamLocation;
import cwms.radar.data.dto.basinconnectivity.StreamReach;
import cwms.radar.data.dto.basinconnectivity.buildercontracts.Build;

import java.util.Collection;

public interface BuildStream extends Build
{
    BuildStream withStreamLocations(Collection<StreamLocation> streamLocations);
    BuildStream withStreamReaches(Collection<StreamReach> streamReaches);
    BuildStream withTributaries(Collection<Stream> tributaries);
    BuildConfluenceStation withReceivingStreamId(String receivingStreamId);
    BuildDiversionStation withDivertingStreamId(String divertingStreamId);
}
