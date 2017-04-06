package solidtea.exceptions;

public class NullDBConnectionException extends NullPointerException {
    public NullDBConnectionException() {
        super("No Database connection has been established");
    }

    public NullDBConnectionException(String err) {
        super(err);
    }
}
