package cwms.cda.formatters.xml;

import org.junit.jupiter.api.Test;

import cwms.cda.formatters.OutputFormatter;
import cwms.cda.formatters.TimeSeriesTestBase;
import cwms.cda.formatters.xml.XMLv2;


public class XMLV2Test extends TimeSeriesTestBase {

    @Override
    public OutputFormatter getOutputFormatter() {
        return new XMLv2();
    }

    @Test
    @Override
    public void singleTimeseriesFormat() {
        super.singleTimeseriesFormat();
    }
}
