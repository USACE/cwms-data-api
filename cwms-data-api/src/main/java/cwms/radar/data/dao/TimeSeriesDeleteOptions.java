package cwms.radar.data.dao;

import java.util.Date;

public interface TimeSeriesDeleteOptions {

    Date getStartTime();

    Date getEndTime();

    boolean isStartTimeInclusive();

    boolean isEndTimeInclusive();

    Date getVersionDate();

    Boolean getMaxVersion();

    Integer getTsItemMask();

    String getOverrideProtection();
}
