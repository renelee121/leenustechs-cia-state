package mx.com.leenustechs.ciaState.models;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import mx.com.leenustechs.ciaState.config.JacksonConfig;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import tools.jackson.databind.json.JsonMapper;

class CommonModelTest {
    private final JsonMapper mapper = new JacksonConfig().objectMapper();

    @Test
    void legacyEnvelopeRemainsReadableAndValid() {
        CommonModel event = mapper.readValue("""
                {"transactionId":"tx-1","producer":"CIA_API","command":"LOGIN","payload":{"user":"test"}}
                """, CommonModel.class);
        assertThatCode(event::validate).doesNotThrowAnyException();
        assertThat(event.getPayload().get("user").asText()).isEqualTo("test");
        assertThat(event.getExecutionId()).isNull();
        assertThat(event.getEventId()).isNull();
        assertThat(event.getOutcome()).isNull();
    }

    @Test
    void explicitCorrelationAndOutcomeSurviveJsonRoundtrip() {
        CommonModel event = valid();
        event.setEventId("event-123");
        event.setExecutionId("execution-456");
        event.setStageIndex(2);
        event.setOutcome(EventOutcome.FAILURE);
        assertThatCode(event::validate).doesNotThrowAnyException();
        assertThat(mapper.readValue(mapper.writeValueAsBytes(event), CommonModel.class)).isEqualTo(event);
    }

    @Test
    void identifiersRejectFirebasePathsAndControlCharacters() {
        for (String bad : List.of("", " ", "nested/tx", "tx.invalid", "tx#", "tx$", "tx[", "tx]", "tx\n", "tx\u007f", "x".repeat(769), "é".repeat(385))) {
            CommonModel event = valid();
            event.setTransactionId(bad);
            assertThatThrownBy(event::validate).as("invalid transactionId %s", bad)
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("transactionId");
        }
        CommonModel event = valid();
        event.setTransactionId(null);
        assertThatThrownBy(event::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validatesOptionalIdentifiersAndRequiredRoutingFields() {
        List<Consumer<CommonModel>> mutations = List.of(
                e -> e.setProducer(null), e -> e.setProducer(" "), e -> e.setProducer("x".repeat(129)),
                e -> e.setCommand(null), e -> e.setEventId("event/path"),
                e -> e.setExecutionId("bad.execution"), e -> e.setStageIndex(-1));
        for (Consumer<CommonModel> mutation : mutations) {
            CommonModel event = valid();
            mutation.accept(event);
            assertThatThrownBy(event::validate).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void acceptsBoundaryLengthAndZeroStage() {
        CommonModel event = valid();
        event.setTransactionId("é".repeat(384));
        event.setProducer("x".repeat(128));
        event.setStageIndex(0);
        assertThatCode(event::validate).doesNotThrowAnyException();
    }

    private CommonModel valid() {
        return new CommonModel("tx-1", "SECURITY", OperationType.LOGIN, mapper.createObjectNode().put("allowed", true));
    }
}
