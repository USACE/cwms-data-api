package cwms.radar.formatters;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class ContentType implements Comparable<ContentType> {
    private static Logger logger = Logger.getLogger(ContentType.class.getName());
    private String contentType;
    private Map<String,String> parameters;

    public ContentType(String contentTypeHeader){
        parameters = new HashMap<>();
        String parts[] = contentTypeHeader.split(";");
        contentType = parts[0];
        if( parts.length > 1){
            for( int i = 1; i < parts.length; i++){
                String key_val[] = parts[i].split("=");
                parameters.put(key_val[0],key_val[1]);
            }
        }

    }

    public String getType(){ return contentType; }
    public Map<String,String> getParameters() {
        return new HashMap<>(parameters);
    }

    @Override
    public boolean equals(Object other){
        logger.finest("Checking + " + this.toString() + " vs " + other.toString());
        if(!(other instanceof ContentType) ) return false;
        ContentType o = (ContentType)other;
        if(!(contentType.equals(o.contentType))) return false;
        for( String key: parameters.keySet() ){
            if( key.equals("q")) continue; // we don't care about q for equals
            if( !parameters.get(key).equals(o.parameters.get(key))) return false;
        }
        return true;
    }

    @Override
    public int hashCode(){
        return this.toString().hashCode();
    }

    @Override
    public int compareTo(ContentType o) {
        float myPriority = Float.parseFloat(parameters.getOrDefault("q", "1"));
        float otherPriority = Float.parseFloat(parameters.getOrDefault("q", "1"));
        if( myPriority == otherPriority) return 0;
        else if( myPriority > otherPriority ) return 1;
        else return -1;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder(contentType);
        for( String key: parameters.keySet()){
            if( key.equals("q")) continue;
            builder.append(";").append(key).append("=").append(parameters.get(key));
        }
        return builder.toString();
    }
}
