package cwms.radar.data.dao;

import cwms.radar.data.dto.Stream;
import cwms.radar.data.dto.StreamLocation;
import cwms.radar.data.dto.StreamReach;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import usace.cwms.db.jooq.codegen.packages.cwms_stream.RETRIEVE_STREAM;

import java.util.HashSet;
import java.util.Set;

import static usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE.*;

public class StreamDao extends JooqDao<Stream>
{
    public StreamDao(DSLContext dsl)
    {
        super(dsl);
    }

    public Stream getStream(String streamId, String stationUnit, String officeId)
    {
        RETRIEVE_STREAM ret = call_RETRIEVE_STREAM(dsl.configuration(), streamId, stationUnit, officeId);
        Boolean startsDownstream = "Downstream".equalsIgnoreCase(ret.getP_STATIONING_STARTS_DS());
        String divertingStream = ret.getP_DIVERTS_FROM_STREAM();
        String receivingStream = ret.getP_FLOWS_INTO_STREAM();
        String diversionBank = ret.getP_DIVERTS_FROM_BANK();
        String confluenceBank = ret.getP_FLOWS_INTO_BANK();
        Double streamLength = ret.getP_LENGTH();
        return new Stream(streamId, startsDownstream, divertingStream, receivingStream, diversionBank, confluenceBank, streamLength,
                getStreamLocationsOnStream(streamId, officeId), getTributaries(streamId, officeId), getReaches(streamId, officeId));
    }

    private Set<StreamLocation> getStreamLocationsOnStream(String streamId, String officeId)
    {
        StreamLocationDao streamLocationDao = new StreamLocationDao(dsl);
        return streamLocationDao.getStreamLocations(streamId, officeId);
    }

    private Set<StreamReach> getReaches(String streamId, String officeId)
    {
        StreamReachDao streamReachDao = new StreamReachDao(dsl);
        return streamReachDao.getReachesOnStream(streamId, officeId);
    }

    private Set<Stream> getTributaries(String streamId, String officeId)
    {
        Set<Stream> retval = new HashSet<>();
        Result<Record> result = call_CAT_STREAMS(dsl.configuration(), null, "km", null, streamId, null, null, null, null, null, null, null, null, null, null, null, null, officeId);
        if(!result.isEmpty())
        {
            for (Record rec : result)
            {
                Boolean startsDownstream = "Downstream".equalsIgnoreCase(rec.get("ZERO_STATION").toString());
                String tributaryId = convertObjectToString(rec.get("LOCATION_ID"));
                String divertingStream = convertObjectToString(rec.get("DIVERTING_STREAM_ID"));
                String receivingStream = convertObjectToString(rec.get("RECEIVING_STREAM_ID"));
                String diversionBank = convertObjectToString(rec.get("DIVERSION_BANK")); //"L" or "R"
                String confluenceBank = convertObjectToString(rec.get("CONFLUENCE_BANK"));
                Double streamLength = convertObjectToDouble(rec.get("STREAM_LENGTH"));
                Stream tributary = new Stream(tributaryId, startsDownstream, divertingStream, receivingStream, diversionBank, confluenceBank,
                        streamLength, getStreamLocationsOnStream(tributaryId, officeId), getTributaries(tributaryId, officeId), getReaches(streamId, officeId));
            }
        }
        return retval;
    }

    private String convertObjectToString(Object obj)
    {
        String retval = null;
        if(obj != null)
        {
            retval = obj.toString();
        }
        return retval;
    }

    private Double convertObjectToDouble(Object obj)
    {
        Double retval = null;
        if(obj instanceof Double)
        {
            retval = Double.parseDouble(obj.toString());
        }
        return retval;
    }
}
