package mx.com.leenustechs.ciaState.business.services.impl;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;

import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.exceptions.TransactionNotFoundException;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.OutboxEvent;
import mx.com.leenustechs.ciaState.models.records.StageResultSnapshot;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.JsonNodeFactory;

/** All workflow decisions and publications are committed in one RTDB transaction. */
@Service
public class EventStateServiceImpl implements EventStateService {
    private final EventStateRepository eventStateRepository;
    private final ObjectMapper objectMapper;
    private final StagePayloadAggregator aggregator = new StagePayloadAggregator();
    private final Clock clock;
    private final long stageTimeoutSeconds;

    public EventStateServiceImpl(EventStateRepository repository, ObjectMapper mapper) {
        this(repository, mapper, Clock.systemUTC(), 300);
    }

    @Autowired
    public EventStateServiceImpl(EventStateRepository repository, ObjectMapper mapper, Clock clock,
            @Value("${workflows.stage-timeout-seconds:300}") long stageTimeoutSeconds) {
        if (stageTimeoutSeconds <= 0) throw new IllegalArgumentException("Stage timeout must be positive");
        try {
            clock.instant().plusSeconds(stageTimeoutSeconds).toEpochMilli();
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("Stage timeout is outside the supported timestamp range");
        }
        this.eventStateRepository = repository;
        this.objectMapper = mapper;
        this.clock = clock;
        this.stageTimeoutSeconds = stageTimeoutSeconds;
    }

    @Override
    public boolean reconcile(String transactionId) {
        Instant now = clock.instant();
        return eventStateRepository.updateAtomically(transactionId, current -> current.flatMap(state -> {
            if (state.getStatus() != TransactionStatus.PROCESSING) {
                if (state.getStageDeadlineAt() == null) return Optional.empty();
                state.setStageDeadlineAt(null);
                return Optional.of(state);
            }
            if (state.getStageDeadlineAt() == null) {
                // Legacy workflows receive a full grace period from first reconciliation.
                state.setStageDeadlineAt(now.plusSeconds(stageTimeoutSeconds));
                return Optional.of(state);
            }
            if (!expired(state, now)) return Optional.empty();
            timeout(state, now);
            return Optional.of(state);
        })).applied();
    }

    private boolean expired(EventStateEntity state, Instant now) {
        return state.getStatus() == TransactionStatus.PROCESSING && state.getStageDeadlineAt() != null
                && !state.getStageDeadlineAt().isAfter(now);
    }

    private void timeout(EventStateEntity state, Instant now) {
        state.setUpdatedAt(now);
        if (state.getSteps() != null) {
            var steps = new EnumMap<StepType, StepStatus>(StepType.class);
            steps.putAll(state.getSteps());
            steps.replaceAll((step, status) -> status == StepStatus.PROCESSING ? StepStatus.ERROR : status);
            state.setSteps(steps);
        }
        fail(state, JsonNodeFactory.instance.objectNode().put("error_code", "STAGE_TIMEOUT")
                .put("error_message", "Workflow stage exceeded its deadline"));
    }

    @Override
    public EventStateResponse findByTransactionId(String transactionId) {
        return toResponse(eventStateRepository.findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId)));
    }

    @Override
    public EventStateResponse process(CommonModel event, List<WorkflowStage> workflow) {
        if (event == null) throw new IllegalArgumentException("Missing workflow event");
        event.validate();
        validate(event, workflow);
        if ("CIA_API".equals(event.getProducer()) && (event.getOutcome() == EventOutcome.FAILURE
                || event.getStageIndex() != null && event.getStageIndex() != 0)) {
            throw new IllegalArgumentException("Initial request must start at stage zero without a failure outcome");
        }
        Instant now = clock.instant();
        return toResponse(eventStateRepository.updateAtomically(event.getTransactionId(), current -> {
            if ("CIA_API".equals(event.getProducer())) {
                if (current.isPresent()) {
                    EventStateEntity existing = current.get();
                    validateReplay(existing, event);
                    if (expired(existing, now)) {
                        timeout(existing, now);
                        return Optional.of(existing);
                    }
                    return Optional.empty();
                }
                EventStateEntity created = create(event, workflow.getFirst(), now);
                enqueueStage(created, workflow.getFirst());
                return Optional.of(created);
            }
            if (current.isEmpty()) {
                return Optional.empty();
            }
            EventStateEntity state = current.get();
            validateCorrelation(state, event);
            if (expired(state, now)) {
                timeout(state, now);
                return Optional.of(state);
            }
            StepType step = StepType.valueOf(event.getProducer());
            int index = stageOf(step, workflow);
            if (index < 0 || (event.getStageIndex() != null && event.getStageIndex() != index)) {
                throw new IllegalArgumentException("Response does not match its declared workflow stage");
            }
            if (state.getStatus() != TransactionStatus.PROCESSING || index < state.getCurrentStage()) {
                return Optional.empty();
            }
            if (index != state.getCurrentStage() || state.getSteps() == null
                    || !state.getSteps().containsKey(step)) {
                throw new IllegalArgumentException("Response belongs to an inactive workflow stage");
            }
            if (state.getSteps().get(step) != StepStatus.PROCESSING) {
                return Optional.empty();
            }
            state.setUpdatedAt(now);
            state.setSteps(new EnumMap<>(state.getSteps()));
            if (isFailure(event)) {
                state.getSteps().put(step, StepStatus.ERROR);
                fail(state, event.getPayload());
                return Optional.of(state);
            }
            state.getSteps().put(step, StepStatus.COMPLETE);
            removeCompletedCommand(state, index, step);
            state.setStepResults(state.getStepResults() == null ? new LinkedHashMap<>()
                    : new LinkedHashMap<>(state.getStepResults()));
            state.getStepResults().put(resultKey(index, step), event.getPayload() == null
                    ? "null" : event.getPayload().toString());
            WorkflowStage stage = workflow.get(index);
            if (!stage.steps().stream().allMatch(s -> state.getSteps().get(s) == StepStatus.COMPLETE)) {
                return Optional.of(state);
            }
            JsonNode merged;
            try {
                Map<StepType, JsonNode> results = new EnumMap<>(StepType.class);
                for (StepType stageStep : stage.steps()) {
                    results.put(stageStep, objectMapper.readTree(state.getStepResults().get(resultKey(index, stageStep))));
                }
                merged = aggregator.aggregate(new StageResultSnapshot(state.getPayload(), results), stage);
            } catch (RuntimeException conflict) {
                // Never include payload fragments or exceptions in the durable/public error.
                fail(state, JsonNodeFactory.instance.objectNode()
                        .put("error_code", "INVALID_STAGE_RESULTS")
                        .put("error_message", "Stage results could not be combined"));
                return Optional.of(state);
            }
            state.setCompletedStages(state.getCompletedStages() == null ? new LinkedHashMap<>()
                    : new LinkedHashMap<>(state.getCompletedStages()));
            state.getCompletedStages().put(index, StepStatus.COMPLETE);
            state.setPayload(copy(merged));
            if (index + 1 == workflow.size()) {
                state.setStatus(TransactionStatus.COMPLETED);
                state.setStageDeadlineAt(null);
                state.setResult(copy(merged));
                enqueueFinal(state, EventOutcome.SUCCESS);
            } else {
                state.setCurrentStage(index + 1);
                state.setStageDeadlineAt(now.plusSeconds(stageTimeoutSeconds));
                state.setSteps(processing(workflow.get(index + 1)));
                enqueueStage(state, workflow.get(index + 1));
            }
            return Optional.of(state);
        }).state().orElseThrow(() -> new TransactionNotFoundException(event.getTransactionId())));
    }

    private EventStateEntity create(CommonModel event, WorkflowStage first, Instant now) {
        EventStateEntity state = new EventStateEntity();
        state.setTransactionId(event.getTransactionId());
        state.setExecutionId(event.getExecutionId() == null ? event.getTransactionId() : event.getExecutionId());
        state.setProducer("CIA_API");
        state.setCommand(event.getCommand());
        state.setInitialPayload(copy(event.getPayload()));
        state.setPayload(copy(event.getPayload()));
        state.setStatus(TransactionStatus.PROCESSING);
        state.setCurrentStage(0);
        state.setStageDeadlineAt(now.plusSeconds(stageTimeoutSeconds));
        state.setSteps(processing(first));
        state.setStepResults(new LinkedHashMap<>());
        state.setCompletedStages(new LinkedHashMap<>());
        state.setPendingOutbox(new LinkedHashMap<>());
        state.setCreatedAt(now);
        state.setUpdatedAt(now);
        return state;
    }

    private void fail(EventStateEntity state, JsonNode result) {
        state.setStatus(TransactionStatus.FAILED);
        state.setStageDeadlineAt(null);
        state.setResult(copy(result));
        enqueueFinal(state, EventOutcome.FAILURE);
    }

    private void enqueueStage(EventStateEntity state, WorkflowStage stage) {
        for (StepType step : stage.steps()) {
            enqueue(state, state.getCurrentStage() + ":" + step.name(), step.getTopic(), state.getPayload(), EventOutcome.SUCCESS);
        }
    }

    private void enqueueFinal(EventStateEntity state, EventOutcome outcome) {
        // Terminal state and cancellation commit together: only the final notification
        // may remain pending after this transition. Sends already in flight cannot be recalled.
        state.setPendingOutbox(new LinkedHashMap<>());
        enqueue(state, "final", KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE, state.getResult(), outcome);
    }

    private void removeCompletedCommand(EventStateEntity state, int stage, StepType step) {
        if (state.getPendingOutbox() == null) {
            return;
        }
        var remaining = new LinkedHashMap<>(state.getPendingOutbox());
        // Receiving this result proves execution even when the Kafka send acknowledgement
        // was lost. Do not retry its command while waiting for other parallel workers.
        remaining.remove(outboxEventId(state, stage + ":" + step.name()));
        state.setPendingOutbox(remaining);
        state.setOutboxPending(!remaining.isEmpty());
    }

    private String outboxEventId(EventStateEntity state, String slot) {
        String execution = state.getExecutionId() == null ? state.getTransactionId() : state.getExecutionId();
        String transaction = state.getTransactionId();
        // Length-prefixed tuple encoding keeps identifiers containing ':' unambiguous.
        String identity = transaction.length() + ":" + transaction + execution.length() + ":" + execution
                + slot.length() + ":" + slot;
        return UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private void enqueue(EventStateEntity state, String slot, String topic, JsonNode payload, EventOutcome outcome) {
        String execution = state.getExecutionId() == null ? state.getTransactionId() : state.getExecutionId();
        String id = outboxEventId(state, slot);
        CommonModel outgoing = new CommonModel(state.getTransactionId(), "CIA_STATE", state.getCommand(), copy(payload));
        outgoing.setEventId(id);
        outgoing.setExecutionId(execution);
        outgoing.setStageIndex(state.getCurrentStage());
        outgoing.setOutcome(outcome);
        state.setPendingOutbox(state.getPendingOutbox() == null ? new LinkedHashMap<>()
                : new LinkedHashMap<>(state.getPendingOutbox()));
        state.getPendingOutbox().putIfAbsent(id, new OutboxEvent(id, topic, state.getTransactionId(), outgoing, state.getUpdatedAt()));
        state.setOutboxPending(!state.getPendingOutbox().isEmpty());
    }

    private void validateReplay(EventStateEntity state, CommonModel event) {
        validateCorrelation(state, event);
        // Legacy states do not retain the original input; never restart them.
        if (state.getExecutionId() != null && !Objects.equals(state.getInitialPayload(), event.getPayload())) {
            throw new IllegalArgumentException("Transaction ID already belongs to a different request");
        }
    }

    private void validateCorrelation(EventStateEntity state, CommonModel event) {
        if (state.getCommand() != event.getCommand()) {
            throw new IllegalArgumentException("Transaction command does not match");
        }
        String stored = state.getExecutionId() == null ? state.getTransactionId() : state.getExecutionId();
        String received = event.getExecutionId() == null ? event.getTransactionId() : event.getExecutionId();
        if (!Objects.equals(stored, received)) {
            throw new IllegalArgumentException("Workflow execution does not match");
        }
    }

    private void validate(CommonModel event, List<WorkflowStage> workflow) {
        if (event == null || event.getTransactionId() == null || event.getTransactionId().isBlank()
                || event.getTransactionId().matches(".*[.#$\\[\\]/\\x00-\\x1f\\x7f].*")
                || event.getProducer() == null || event.getCommand() == null) {
            throw new IllegalArgumentException("Invalid workflow envelope");
        }
        if (workflow == null || workflow.isEmpty()) {
            throw new IllegalArgumentException("Workflow must contain stages");
        }
        HashSet<StepType> seen = new HashSet<>();
        for (WorkflowStage stage : workflow) {
            if (stage == null || stage.steps() == null || stage.steps().isEmpty()) {
                throw new IllegalArgumentException("Workflow must contain nonempty stages");
            }
            for (StepType step : stage.steps()) {
                if (step == null || !seen.add(step)) {
                    throw new IllegalArgumentException("Workflow steps must be unique");
                }
            }
        }
        if (!"CIA_API".equals(event.getProducer())) {
            StepType.valueOf(event.getProducer());
        }
    }

    private int stageOf(StepType step, List<WorkflowStage> stages) {
        for (int i = 0; i < stages.size(); i++) {
            if (stages.get(i).steps().contains(step)) return i;
        }
        return -1;
    }

    private boolean isFailure(CommonModel event) {
        if (event.getOutcome() != null) return event.getOutcome() == EventOutcome.FAILURE;
        JsonNode p = event.getPayload();
        return p != null && (p.has("exception") || (p.has("error_code") && p.has("error_message")));
    }

    private Map<StepType, StepStatus> processing(WorkflowStage stage) {
        Map<StepType, StepStatus> steps = new EnumMap<>(StepType.class);
        stage.steps().forEach(step -> steps.put(step, StepStatus.PROCESSING));
        return steps;
    }

    private String resultKey(int stage, StepType step) { return stage + ":" + step.name(); }
    private JsonNode copy(JsonNode node) { return node == null ? null : node.deepCopy(); }

    private EventStateResponse toResponse(EventStateEntity state) {
        EventStateResponse response = new EventStateResponse();
        response.setTransactionId(state.getTransactionId());
        response.setCommand(state.getCommand());
        response.setStatus(state.getStatus());
        response.setCurrentStage(state.getCurrentStage());
        if (state.getSteps() != null) {
            Map<StepType, StepStatus> steps = new EnumMap<>(StepType.class);
            steps.putAll(state.getSteps());
            response.setSteps(steps);
        }
        response.setResult(copy(state.getResult()));
        return response;
    }
}
