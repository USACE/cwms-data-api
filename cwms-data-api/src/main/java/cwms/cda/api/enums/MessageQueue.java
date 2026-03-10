package cwms.cda.api.enums;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    name = "Message Queue",
    description = "Desired set of status messages. Must be one of the named options."
                + " Letter casing is ignored."
)
public enum MessageQueue {
    TS_STORED("TS_STORED", " CWMS messages about time series operations, such as data stored and deleted"),
    STATUS("STATUS", " CWMS general system and application status messages"),
    REALTIME_OPS("REALTIME_OPS", " CWMS application operational messages");

    private String queue;
    private String description;

    MessageQueue(String queue, String description) {
        this.queue = queue;
        this.description = description;
    }

    public String value() {
        return queue;
    }

    public String description() {
        return description;
    }

    public static MessageQueue queueFor(String queue) {
        if (TS_STORED.value().equalsIgnoreCase(queue)) {
            return TS_STORED;
        } else if (STATUS.value().equalsIgnoreCase(queue)) {
            return STATUS;
        } else if (REALTIME_OPS.value().equalsIgnoreCase(queue)) {
            return REALTIME_OPS;
        } else {
            return null;
        }
    }
}
