package cwms.radar.api.graph.basinconnectivity.nodes;

import javax.validation.constraints.NotNull;
import java.util.Comparator;

public class BasinConnectivityNodeComparator implements Comparator<BasinConnectivityNode>
{
    private final boolean startsDownstream;

    public BasinConnectivityNodeComparator(boolean startsDownstream)
    {
        this.startsDownstream = startsDownstream;
    }

    @Override
    public int compare(@NotNull BasinConnectivityNode o1, @NotNull BasinConnectivityNode o2)
    {
        int retval;
        if(!startsDownstream)
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
