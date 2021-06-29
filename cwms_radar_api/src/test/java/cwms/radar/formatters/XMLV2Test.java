package cwms.radar.formatters;

import org.junit.jupiter.api.Test;

import cwms.radar.formatters.xml.XMLv2;

public class XMLV2Test extends TimeSeriesTestBase {

    @Override
    public OutputFormatter getOutputFormatter() {
        return new XMLv2();
    }

    @Test
    @Override
    public void SingleTimeseriesFormat() {
        super.SingleTimeseriesFormat();
    }
}
