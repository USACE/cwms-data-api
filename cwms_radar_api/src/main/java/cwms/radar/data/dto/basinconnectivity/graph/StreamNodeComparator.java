package cwms.radar.data.dto.basinconnectivity.graph;

import javax.validation.constraints.NotNull;
import java.util.Comparator;

public class StreamNodeComparator implements Comparator<BasinConnectivityNode>
{
    private final boolean _startsDownstream;

    public StreamNodeComparator(boolean startsDownstream)
    {
        _startsDownstream = startsDownstream;
    }

    @Override
    public int compare(@NotNull BasinConnectivityNode o1, @NotNull BasinConnectivityNode o2)
    {
        int retval;
        if(!_startsDownstream)
        {
           retval = o1.getStation().compareTo(o2.getStation());
        }
        else
        {
            retval = o2.getStation().compareTo(o1.getStation());
        }
        return retval;
    }
}
