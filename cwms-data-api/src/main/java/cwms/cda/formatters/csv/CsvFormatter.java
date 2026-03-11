package cwms.cda.formatters.csv;

import cwms.cda.data.dto.csv.CwmsCsvDTOBase;
import cwms.cda.formatters.OutputFormatter;

import java.util.List;

public interface CsvFormatter extends OutputFormatter {
    String formatWithMetaDataIncludedAsColumns(CwmsCsvDTOBase dto);
    String formatWithMetaDataIncludedAsComments(CwmsCsvDTOBase dto);
}
