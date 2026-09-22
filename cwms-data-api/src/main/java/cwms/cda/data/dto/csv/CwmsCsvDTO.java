package cwms.cda.data.dto.csv;

import java.util.List;

public interface CwmsCsvDTO<T> {
    List<T> getRows();
}
