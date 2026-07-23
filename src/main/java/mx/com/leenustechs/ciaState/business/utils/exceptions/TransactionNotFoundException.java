package mx.com.leenustechs.ciaState.business.utils.exceptions;

public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(String transactionId) {
        super("No hay registros con el id de transacción: " + transactionId);
    }
}
