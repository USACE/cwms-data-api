package cwms.radar.data.util;

import java.util.Arrays;
import java.util.List;

public class BasinUnitsConverter
{
    private static final double FEET_TO_METER_CONVERSION_FACTOR = 12.0 * 2.54 / 100.0;
    private static final double METER_TO_FEET_CONVERSION_FACTOR = 1/FEET_TO_METER_CONVERSION_FACTOR;
    private static final double KM2_TO_MILE2_CONVERSION_FACTOR = 0.386102159;
    private static final double MILE2_TO_KM2_CONVERSION_FACTOR = 1/KM2_TO_MILE2_CONVERSION_FACTOR;
    private static final double KM_TO_MILE_CONVERSION_FACTOR = 0.6213711922;
    private static final double MILE_TO_KM_CONVERSION_FACTOR = 1/KM_TO_MILE_CONVERSION_FACTOR;
    private static final String METER_UNITS = "m";
    private static final String FEET_UNITS = "ft";
    private static final String KM_UNITS = "km";
    private static final String MILE_UNITS = "mi";
    private static final String SQUARE_KM_UNITS = "km2";
    private static final String SQUARE_MILE_UNITS = "mi2";
    private static final List<String> ELEV_UNITS = Arrays.asList(METER_UNITS, FEET_UNITS);
    private static final List<String> DISTANCE_UNITS = Arrays.asList(KM_UNITS, MILE_UNITS);
    private static final List<String> AREA_UNITS = Arrays.asList(SQUARE_KM_UNITS, SQUARE_MILE_UNITS);
    private static final int DEFAULT_PRECISION = 4;

    private BasinUnitsConverter(){}

    public static Double convertUnits(Double val, String unitsFrom, String unitsTo, int precision)
    {
        double precisionFactor = Math.pow(10.0d, precision);
        Double retval = val;
        validateUnits(unitsFrom, unitsTo);
        if(val != null && !unitsFrom.equalsIgnoreCase(unitsTo))
        {
            Double convertedVal = convertValidatedUnits(val, unitsFrom);
            retval = Math.round(convertedVal * precisionFactor) / precisionFactor;
        }
        return retval;
    }

    public static Double convertUnits(Double val, String unitsFrom, String unitsTo) throws IllegalArgumentException
    {
        return convertUnits(val, unitsFrom, unitsTo, DEFAULT_PRECISION);
    }

    private static Double convertValidatedUnits(Double val, String unitsFrom)
    {
        Double retval = val;
        if(unitsFrom.equalsIgnoreCase(METER_UNITS))
        {
            retval = meterToFeet(val);
        }
        else if(unitsFrom.equalsIgnoreCase(FEET_UNITS))
        {
            retval = feetToMeter(val);
        }
        else if(unitsFrom.equalsIgnoreCase(KM_UNITS))
        {
            retval = kmToMile(val);
        }
        else if(unitsFrom.equalsIgnoreCase(MILE_UNITS))
        {
            retval = mileToKm(val);
        }
        else if(unitsFrom.equalsIgnoreCase(SQUARE_KM_UNITS))
        {
            retval = km2ToMi2(val);
        }
        else if(unitsFrom.equalsIgnoreCase(SQUARE_MILE_UNITS))
        {
            retval = mi2ToKm2(val);
        }
        return retval;
    }

    private static void validateUnits(String unitsFrom, String unitsTo) throws IllegalArgumentException
    {
        if(unitsFrom == null || unitsTo == null)
        {
            throw new IllegalArgumentException("cannot convert NULL unit");
        }
        unitsFrom = unitsFrom.toLowerCase();
        unitsTo = unitsTo.toLowerCase();
        if(!ELEV_UNITS.contains(unitsFrom) && !DISTANCE_UNITS.contains(unitsFrom) && !AREA_UNITS.contains(unitsFrom))
        {
            throw new IllegalArgumentException("Unit provided (" + unitsFrom + ") is not supported");
        }
        if(!ELEV_UNITS.contains(unitsTo) && !DISTANCE_UNITS.contains(unitsTo) && !AREA_UNITS.contains(unitsTo))
        {
            throw new IllegalArgumentException("Unit provided (" + unitsTo + ") is not supported");
        }
        if(ELEV_UNITS.contains(unitsFrom))
        {
            validatedUnitsToIsElevation(unitsFrom, unitsTo);
        }
        if(DISTANCE_UNITS.contains(unitsFrom))
        {
            validateUnitsToIsDistance(unitsFrom, unitsTo);
        }
        if(AREA_UNITS.contains(unitsFrom))
        {
            validateUnitsToIsArea(unitsFrom, unitsTo);
        }
    }

    private static void validatedUnitsToIsElevation(String unitsFrom, String unitsTo)
    {
        if(DISTANCE_UNITS.contains(unitsTo))
        {
            throw new IllegalArgumentException("Cannot convert elevation units (" + unitsFrom + ") to distance units (" + unitsTo + ")");
        }
        else if(AREA_UNITS.contains(unitsTo))
        {
            throw new IllegalArgumentException("Cannot convert elevation units (" + unitsFrom + ") to area units (" + unitsTo + ")");
        }
    }

    private static void validateUnitsToIsDistance(String unitsFrom, String unitsTo)
    {
        if (ELEV_UNITS.contains(unitsTo))
        {
            throw new IllegalArgumentException("Cannot convert distance units (" + unitsFrom + ") to elevation units (" + unitsTo + ")");
        }
        else if (AREA_UNITS.contains(unitsTo))
        {
            throw new IllegalArgumentException("Cannot convert distance units (" + unitsFrom + ") to area units (" + unitsTo + ")");
        }
    }

    private static void validateUnitsToIsArea(String unitsFrom, String unitsTo)
    {
        if (ELEV_UNITS.contains(unitsTo))
        {
            throw new IllegalArgumentException("Cannot convert area units (" + unitsFrom + ") to elevation units (" + unitsTo + ")");
        }
        else if (DISTANCE_UNITS.contains(unitsTo))
        {
            throw new IllegalArgumentException("Cannot convert area units (" + unitsFrom + ") to distance units (" + unitsTo + ")");
        }
    }

    private static Double meterToFeet(Double meter)
    {
        return METER_TO_FEET_CONVERSION_FACTOR * meter;
    }

    private static Double feetToMeter(Double ft)
    {
        return FEET_TO_METER_CONVERSION_FACTOR * ft;
    }

    private static Double kmToMile(Double km)
    {
        return KM_TO_MILE_CONVERSION_FACTOR * km;
    }

    private static Double mileToKm(Double mile)
    {
        return MILE_TO_KM_CONVERSION_FACTOR * mile;
    }

    private static Double km2ToMi2(Double km2)
    {
        return KM2_TO_MILE2_CONVERSION_FACTOR * km2;
    }

    private static Double mi2ToKm2(Double mi2)
    {
        return MILE2_TO_KM2_CONVERSION_FACTOR * mi2;
    }
}
