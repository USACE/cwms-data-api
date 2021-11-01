package cwms.radar.security;

public class CwmsAuthException extends RuntimeException {

    public CwmsAuthException(String msg){
        super(msg);
    }

    public CwmsAuthException(String msg, Throwable err) {
        super(msg,err);
    }

}
