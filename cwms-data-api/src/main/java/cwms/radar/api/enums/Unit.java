package cwms.radar.api.enums;

public enum Unit {
    KILOMETER("km"),
    MILE("mi"),
    SQUARE_KILOMETERS("km2"),
    SQUARE_MILES("mi2"),
    METER("m"),
    FEET("ft"),
    UNDEF("undefined");
    private final String unitStr;

    Unit(String unitStr) {
        this.unitStr = unitStr;
    }

    public String getValue() {
        return unitStr;
    }

    public Unit unitFor(String unitStr) {
        Unit retval = UNDEF;
        for (Unit unit : values()) {
            if (unit.getValue().equalsIgnoreCase(unitStr)) {
                retval = unit;
                break;
            }
        }
        return retval;
    }
}
