package helpers;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.jupiter.api.Test;



public class TsRandomSamplerTest {

    @Test
    public void can_load_timeseries_list() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/cwms/cda/data/timeseries.csv");
        assertNotNull(stream, "unable to load csv file");
        List<TsRandomSampler.TsSample> list = TsRandomSampler.load_data(new InputStreamReader(stream));
        assertFalse(list.isEmpty(), "not timeseries returned from list");
    }
}
