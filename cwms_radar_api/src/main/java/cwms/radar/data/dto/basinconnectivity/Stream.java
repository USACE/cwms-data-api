package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;

import java.util.*;

public final class Stream implements CwmsDTO
{
    private final String _streamId;
    private final List<Stream> _tributaries;//streams that flow into this
    private final List<StreamReach> _streamReaches;
    private final boolean _startsDownstream;
    private final String _divertingStreamId; //flows from
    private final String _receivingStreamId; //flows into
    private final String _confluenceBank;
    private final String _diversionBank;
    private final Double _streamLength;
    private final Double _confluenceStation;
    private final Double _diversionStation;
    private final List<StreamLocation> _streamLocations;

    Stream(StreamBuilder streamBuilder)
    {
        _streamId = streamBuilder.getStreamId();
        _startsDownstream = streamBuilder.startDownstream();
        _divertingStreamId = streamBuilder.getDivertingStreamId();
        _receivingStreamId = streamBuilder.getReceivingStreamId();
        _confluenceBank = streamBuilder.getConfluenceBank();
        _diversionBank = streamBuilder.getDiversionBank();
        _streamLength = streamBuilder.getStreamLength();
        _confluenceStation = streamBuilder.getConfluenceStation();
        _diversionStation = streamBuilder.getDiversionStation();
        _streamLocations = streamBuilder.getStreamLocations();
        _tributaries = streamBuilder.getTributaries();
        _streamReaches = streamBuilder.getStreamReaches();
    }

    public List<StreamLocation> getStreamLocations()
    {
        return new ArrayList<>(_streamLocations);
    }

    public List<StreamReach> getStreamReaches()
    {
        return new ArrayList<>(_streamReaches);
    }

    public List<Stream> getTributaries()
    {
        return new ArrayList<>(_tributaries);
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

    public Double getConfluenceStation()
    {
        return _confluenceStation;
    }

    public Double getDiversionStation()
    {
        return _diversionStation;
    }

}

