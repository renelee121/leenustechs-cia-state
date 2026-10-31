package mx.com.leenustechs.ciaState.models.records;

import java.time.Instant;
import mx.com.leenustechs.ciaState.models.CommonModel;

/** Persisted identity, envelope and creation time of a pending publication. */
public record OutboxEvent(String id, String topic, String key, CommonModel event, Instant enqueuedAt) {
    public OutboxEvent(String id, String topic, String key, CommonModel event) {
        this(id, topic, key, event, null);
    }
}
