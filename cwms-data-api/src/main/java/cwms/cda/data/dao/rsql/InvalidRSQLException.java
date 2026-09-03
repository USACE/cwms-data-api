package cwms.cda.data.dao.rsql;


public class InvalidRSQLException extends IllegalArgumentException {
    public InvalidRSQLException(String message, Throwable e) {
        super(message, e);
    }

    public InvalidRSQLException(String message) {
        super(message);
    }

    public InvalidRSQLException(Throwable e) {
        super(e);
    }

    public InvalidRSQLException() {
        super();
    }

}
