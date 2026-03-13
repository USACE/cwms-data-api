package cwms.cda.formatters.csv;

import cwms.cda.data.dto.csv.CwmsCsvRow;
import cwms.cda.formatters.OutputFormatter;

import java.util.List;

public interface CsvFormatter extends OutputFormatter {
    String formatWithMetaDataIncludedAsColumns(CwmsCsvRow dto);
    String formatWithMetaDataIncludedAsComments(CwmsCsvRow dto);
    String formatWithMetaDataIncludedAsComments(List<? extends CwmsCsvRow> dtoList);
    String formatWithMetaDataIncludedAsColumns(List<? extends CwmsCsvRow> dtoList);
}
