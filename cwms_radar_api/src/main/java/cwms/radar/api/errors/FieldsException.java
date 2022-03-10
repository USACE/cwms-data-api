package cwms.radar.api.errors;

public class FieldsException extends RuntimeException{
    private String message;

    public FieldsException(String message){
        this.message = message;
    }

    public String getMessage(){
        return message;
    }

}
