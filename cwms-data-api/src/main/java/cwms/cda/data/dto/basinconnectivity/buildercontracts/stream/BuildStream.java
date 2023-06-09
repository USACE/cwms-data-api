package cwms.cda.data.dto.basinconnectivity.buildercontracts.stream;

import cwms.cda.data.dto.basinconnectivity.Stream;
import cwms.cda.data.dto.basinconnectivity.StreamLocation;
import cwms.cda.data.dto.basinconnectivity.StreamReach;

import java.util.Collection;

public interface BuildStream
{
    BuildStream withStreamLocations(Collection<StreamLocation> streamLocations);
    BuildStream withStreamReaches(Collection<StreamReach> streamReaches);
    BuildStream withTributaries(Collection<Stream> tributaries);
    BuildConfluenceStation withReceivingStreamId(String receivingStreamId);
    BuildDiversionStation withDivertingStreamId(String divertingStreamId);
    BuildStream withComment(String comment);
    BuildStream withAverageSlope(Double averageSlope);
    Stream build();
}
