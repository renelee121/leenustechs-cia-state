package mx.com.leenustechs.ciaState.models.records;

import mx.com.leenustechs.ciaState.models.CommonModel;

/** Persisted identity and envelope of a publication stored with its workflow. */
public record OutboxEvent(String id, String topic, String key, CommonModel event) { }
