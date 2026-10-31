package mx.com.leenustechs.ciaState.models.records;

/** Exact Firebase numeric/key ordering, including malformed fractional deadline rows. */
public record DeadlineCursor(double deadlineMillis, String transactionId) {
    public DeadlineCursor {
        if (!Double.isFinite(deadlineMillis) || deadlineMillis < 0
                || transactionId == null || transactionId.isEmpty()) {
            throw new IllegalArgumentException("Invalid deadline scan cursor");
        }
    }
}
