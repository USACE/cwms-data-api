package cwms.cda.data.dao.rsql;


public class EmptyQueryException extends InvalidRSQLException {
    public EmptyQueryException(String message, Throwable e) {
        super(message, e);
    }

    public EmptyQueryException(String message) {
        super(message);
    }

    public EmptyQueryException(Throwable e) {
        super(e);
    }

    public EmptyQueryException() {
        super();
    }

}
