package cwms.radar.data.dto;

import cwms.radar.api.graph.pgjson.PgJsonGraph;

public interface PgJsonDTO extends CwmsDTO
{
    PgJsonGraph getPgJsonGraph();
}
