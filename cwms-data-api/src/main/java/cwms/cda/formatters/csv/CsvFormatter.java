package cwms.cda.formatters.csv;

import cwms.cda.data.dto.csv.CwmsCsvDTO;
import cwms.cda.formatters.OutputFormatter;

public interface CsvFormatter extends OutputFormatter {
    String format(CwmsCsvDTO<?> dto, CsvConfiguration config);
}
