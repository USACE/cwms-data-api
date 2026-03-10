package cwms.cda.api.enums;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    name = "Message Queue",
    description = "Desired set of status messages. Must be one of the named options."
                + " Letter casing is ignored."
)
public enum MessageQueue {
    TS_STORED("TS_STORED"),
    STATUS("STATUS"),
    REALTIME_OPS("REALTIME_OPS");

    private String queue;

    MessageQueue(String queue) {
        this.queue = queue;
    }

    public String value() {
        return queue;
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
