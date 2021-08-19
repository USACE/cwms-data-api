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
    private final ArrayList<StreamLocation> _streamLocations;

    //This could probably be refactored to use builder pattern
    public Stream(String streamId, boolean startsDownstream, String divertingStreamId, String receivingStreamId,
                  String diversionBank, String confluenceBank, Double diversionStation, Double confluenceStation,
                  Double streamLength, Set<StreamLocation> streamLocations, Set<Stream> tributaries, Set<StreamReach> reachesOnStream)
    {
        _streamId = streamId;
        _startsDownstream = startsDownstream;
        _divertingStreamId = divertingStreamId;
        _receivingStreamId = receivingStreamId;
        _confluenceBank = confluenceBank;
        _diversionBank = diversionBank;
        _streamLength = streamLength;
        _confluenceStation = confluenceStation;
        _diversionStation = diversionStation;
        _streamLocations = new ArrayList<>(streamLocations);
        _tributaries = new ArrayList<>(tributaries);
        _streamReaches = new ArrayList<>(reachesOnStream);
    }

    public List<StreamLocation> getStreamLocations()
    {
        return _streamLocations;
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

