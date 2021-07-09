package cwms.radar.api.enums;

import java.util.logging.Logger;

import io.swagger.v3.oas.annotations.media.Schema;


@Schema(
    name = "Unit System",
    description = "Unit System desired in response. Can be SI (International Scientific) or EN (Imperial.) If unspecified, defaults to SI."
)
public enum UnitSystem {
    SI("SI"),
    EN("EN");
    public static final String DESCRIPTION = "Unit System desired in response. Can be SI (International Scientific) or EN (Imperial.) If unspecified, defaults to SI.";
    private String dataSet;

    UnitSystem(String value) {
        this.dataSet = value;
    }

    public String value(){
        return dataSet;
    }

    public String getValue(){
        return dataSet;
    }

    public static UnitSystem systemFor(String system){
        if( "SI".equalsIgnoreCase((system))) { return SI; }
        else if( "EN".equalsIgnoreCase(system)) { return EN; }
        else return SI;
    }
}
