package cwms.cda.formatters.csv;

import java.io.InputStream;
import java.util.List;

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;

public class CsvV1 implements OutputFormatter {

    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        String retVal = null;
        if (dto instanceof Office ) {
            retVal = new CsvV1Office().format(dto);
        } else if (dto instanceof LocationGroup ) {
            retVal = new CsvV1LocationGroup().format(dto);
        }
        return retVal;
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        String retVal = null;
        if (dtoList != null && !dtoList.isEmpty()) {
            CwmsDTOBase dto = dtoList.get(0);
            if (dto instanceof Office) {
                retVal = new CsvV1Office().format(dtoList);
            } else if(dto instanceof LocationGroup) {
                retVal = new CsvV1LocationGroup().format(dtoList);
            }

        }
        return retVal;
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        T retVal = null;
        if (type.isAssignableFrom(Office.class)) {
            retVal = new CsvV1Office().parseContent(content, type);
        } else if (type.isAssignableFrom(LocationGroup.class)) {
            retVal = new CsvV1LocationGroup().parseContent(content, type);
        }
        return retVal;
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
