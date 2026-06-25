package cwms.cda.formatters.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.csv.CwmsCsvDTO;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import java.io.InputStream;
import java.util.List;

public class CsvV1 implements CsvFormatter {

    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    /**
     * Default formatting does not include metadata in either columns or comments.
    **/
    @Override
    public String format(CwmsDTOBase dto) {
        try {
            if (dto instanceof Office ) {
                return new CsvV1Office().format(dto);
            } else if (dto instanceof LocationGroup ) {
                return new CsvV1LocationGroup().format(dto);
            } else if (dto instanceof CwmsCsvDTO) {
                return format((CwmsCsvDTO<?>) dto, new CsvConfiguration.Builder().build());
            } else {
                throw new FormattingException(dto.getClass().getName() + " is not currently supported for CSV formatting.");
            }
        } catch (Exception e) {
            throw new FormattingException("Could not serialize:" + dto.getClass().getName(), e);
        }
    }

    @Override
    public String format(CwmsCsvDTO<?> dto, CsvConfiguration config) {
        try {
            return CwmsCsvProcessor.formatCwmsCsv(dto, config);
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not serialize:" + dto.getClass().getName(), e);
        }
    }


    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        if (dtoList != null && !dtoList.isEmpty()) {
            CwmsDTOBase dto = dtoList.get(0);
            if (dto instanceof Office) {
                return new CsvV1Office().format(dtoList);
            } else if (dto instanceof LocationGroup) {
                return new CsvV1LocationGroup().format(dtoList);
            } else {
                throw new FormattingException(dto.getClass().getName() + " is not currently supported for CSV formatting.");
            }
        }
        return null;
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        if (type.isAssignableFrom(Office.class)) {
            return new CsvV1Office().parseContent(content, type);
        } else if (type.isAssignableFrom(LocationGroup.class)) {
            return new CsvV1LocationGroup().parseContent(content, type);
        } else if (CwmsCsvDTO.class.isAssignableFrom(type)) {
            return CwmsCsvProcessor.parseCwmsCsv(content, type);
        }
        return null;
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        T retVal = null;
        if (type.isAssignableFrom(Office.class)) {
            retVal = new CsvV1Office().parseContent(content, type);
        } else if (type.isAssignableFrom(LocationGroup.class)) {
            retVal = new CsvV1LocationGroup().parseContent(content, type);
        }
        return retVal;
    }

}
