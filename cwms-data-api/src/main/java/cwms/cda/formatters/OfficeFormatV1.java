package cwms.cda.formatters;

import cwms.cda.data.dto.Office;
import java.util.List;

public class OfficeFormatV1 {
    public static class OfficesFmt {
        public List<Office> offices;
    }

    public final OfficesFmt offices = new OfficesFmt();

    public OfficeFormatV1(List<Office> offices) {
        this.offices.offices = offices;
    }
}
