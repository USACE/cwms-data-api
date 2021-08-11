package cwms.radar.data.dao;

import cwms.radar.data.dto.StreamLocation;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import java.util.HashSet;
import java.util.Set;
import static usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE.*;

public class StreamLocationDao extends JooqDao<StreamLocation>
{
    public StreamLocationDao(DSLContext dsl)
    {
        super(dsl);
    }

    /**
     *
     * @param streamId - stream containing stream locations that are being retrieved
     * @return list of stream locations on stream
     */
    public Set<StreamLocation> getStreamLocations(String streamId, String officeId)
    {
        Set<StreamLocation> retVal = new HashSet<>();
        String pStreamIdMaskIn = "*";
        String pLocationIdMaskIn = "*";
        String pStationUnitIn = "km";
        String pStageUnitIn = "ft";
        String pAreaUnitIn = "km2";
        if(streamId != null)
        {
            pStreamIdMaskIn = streamId;
        }
        Result<Record> streamLocs = call_CAT_STREAM_LOCATIONS(dsl.configuration(), pStreamIdMaskIn, pLocationIdMaskIn, pStationUnitIn, pStageUnitIn, pAreaUnitIn, officeId);
        for(Record rec : streamLocs)
        {
            Object stationObj = rec.get("STATION");
            Object bankObj = rec.get("BANK");
            String locationId = rec.get("LOCATION_ID").toString();
            double station = Double.NaN;
            String bank = "L";
            if(stationObj != null)
            {
                station = Double.parseDouble(stationObj.toString());
            }
            if(bankObj != null)
            {
                bank = bankObj.toString();
            }
            StreamLocation streamLocation = new StreamLocation(locationId, streamId, station, bank);
            retVal.add(streamLocation);
        }
        return retVal;
    }

    public Set<StreamLocation> getAllStreamLocations(String officeId)
    {
        return getStreamLocations(null, officeId);
    }
}
