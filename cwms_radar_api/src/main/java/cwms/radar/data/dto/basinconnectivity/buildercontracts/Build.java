package cwms.radar.data.dto.basinconnectivity.buildercontracts;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.basinconnectivity.Stream;

public interface Build<T>
{
    public T build();
}
