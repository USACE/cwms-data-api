package cwms.radar.data.dto;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;

public final class Stream implements CwmsDTO
{
    private final String _streamId;
    private final Set<Stream> _tributaries;//streams that flow into this
    private final Set<StreamLocation> _streamLocations; //streamLocations on stream
    private final Set<StreamReach> _streamReaches;
    private final boolean _startsDownstream;
    private final String _divertingStreamId; //flows from
    private final String _receivingStreamId; //flows into
    private final String _confluenceBank;
    private final String _diversionBank;
    private final Double _streamLength;

    public Stream(String streamId, boolean startsDownstream, String divertingStreamId, String receivingStreamId,
                  String diversionBank, String confluenceBank, Double streamLength, Set<StreamLocation> streamLocations,
                  Set<Stream> tributaries, Set<StreamReach> reachesOnStream)
    {
        _streamId = streamId;
        _startsDownstream = startsDownstream;
        _divertingStreamId = divertingStreamId;
        _receivingStreamId = receivingStreamId;
        _confluenceBank = confluenceBank;
        _diversionBank = diversionBank;
        _streamLength = streamLength;
        _streamLocations = streamLocations;
        _tributaries = tributaries;
        _streamReaches = reachesOnStream;
    }

    public Set<StreamLocation> getStreamLocations()
    {
        return new HashSet<>(_streamLocations);
    }

    public Set<StreamReach> getStreamReaches()
    {
        return new HashSet<>(_streamReaches);
    }

    public Set<Stream> getTributaries()
    {
        return new HashSet<>(_tributaries);
    }

    public String getStreamId()
    {
        return _streamId;
    }

    public String getDivertingStreamId()
    {
        return _divertingStreamId;
    }

    public String getReceivingStreamId()
    {
        return _receivingStreamId;
    }

    public boolean startsDownstream()
    {
        return _startsDownstream;
    }

    public String getDiversionBank()
    {
        return _diversionBank;
    }

    public String getConfluenceBank()
    {
        return _confluenceBank;
    }

    public Double getStreamLength()
    {
        return _streamLength;
    }
}
