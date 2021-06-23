package cwms.radar.formatters;

import org.junit.jupiter.api.Test;

import cwms.radar.formatters.json.JsonV2;

public class JsonV2Test extends TimeSeriesTestBase {

    @Override
    public OutputFormatter getOutputFormatter() {
        return new JsonV2();
    }

    @Test
    @Override
    public void SingleTimeseriesFormat() {
        super.SingleTimeseriesFormat();
    }
}
