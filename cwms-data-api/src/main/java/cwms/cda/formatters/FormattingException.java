package cwms.cda.formatters;

public class FormattingException extends RuntimeException{

    public FormattingException(String message){
        super(message);
    }

    public FormattingException(String message, Throwable err ){
        super(message,err);
    }
}
