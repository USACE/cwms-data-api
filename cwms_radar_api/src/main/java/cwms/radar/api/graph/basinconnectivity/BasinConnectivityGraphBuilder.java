package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.data.dto.basinconnectivity.Stream;

public class BasinConnectivityGraphBuilder
{
    private BasinConnectivityGraphBuilder()
    {
        super();
    }

    public static BasinConnectivityGraph buildBasinConnectivityGraph(Basin basin)
    {
        BasinConnectivityGraph retVal = null;
        Stream primaryStream = basin.getPrimaryStream();
        if(primaryStream != null)
        {
            BasinConnectivityStream primaryBasinConnStream = new BasinConnectivityStream(primaryStream, null);
            retVal = new BasinConnectivityGraph(primaryBasinConnStream.getEdges(), primaryBasinConnStream.getNodes());
        }
        return retVal;
    }
}
