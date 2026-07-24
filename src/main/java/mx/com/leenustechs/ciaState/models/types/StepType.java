package mx.com.leenustechs.ciaState.models.types;

import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;

public enum StepType {

    SECURITY(KafkaTopics.LEENUSTECHS_CIA_SECURITY),
    STUDENTS(KafkaTopics.LEENUSTECHS_CIA_STUDENTS),
    ASSETS(KafkaTopics.LEENUSTECHS_CIA_ASSETS),
    PREFERENCES(KafkaTopics.LEENUSTECHS_CIA_PREFERENCES),
    ROLES(KafkaTopics.LEENUSTECHS_CIA_ROLES),
    MODULES(KafkaTopics.LEENUSTECHS_CIA_MODULES);

    private final String topic;

    StepType(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}