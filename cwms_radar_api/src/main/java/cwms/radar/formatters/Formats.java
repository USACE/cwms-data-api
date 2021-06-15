package cwms.radar.formatters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.helpers.ResourceHelper;



public class Formats {
    public static final Logger logger = Logger.getLogger(Formats.class.getName());
    public static final String PLAIN = "text/plain";    // Only used as a constant, not for any data mapping
    public static final String JSON = "application/json";
    public static final String XML = "application/xml";
    public static final String XMLV2 = "application/xml;version=2";
    public static final String WML2 = "application/vnd.opengis.waterml+xml";
    public static final String JSONV2 = "application/json;version=2";
    public static final String TAB = "text/tab-separated-values";
    public static final String CSV = "text/csv";

    private static ArrayList<ContentType> contentTypeList = new ArrayList<>();
    static {
        contentTypeList.addAll(
            Arrays.asList(JSON,XML,WML2,JSONV2,TAB,CSV)
            .stream().map( ct -> new ContentType(ct)).collect(Collectors.toList()));
    }
    private static HashMap<String,String> type_map =new HashMap<>();
    static{
        type_map.put("json",Formats.JSON);
        type_map.put("xml",Formats.XML);
        type_map.put("wml2",Formats.WML2);
        type_map.put("tab",Formats.TAB);
        type_map.put("csv",Formats.CSV);
    };


    private Map<ContentType, Map<Class<CwmsDTO>,OutputFormatter> > formatters = null;

    private static Formats formats = null;

    private Formats() throws IOException{
        formatters = new HashMap<>();
        InputStream formatList = ResourceHelper.getResourceAsStream("/formats.list", this.getClass());
        BufferedReader br = new BufferedReader(new InputStreamReader(formatList));
        while( br.ready() ){
            String line = br.readLine();
            logger.info(line);
            String type_formatter_classes[] = line.split(":");

            ContentType type = new ContentType(type_formatter_classes[0]);
            try {
                @SuppressWarnings("unchecked")
                Class<OutputFormatter> formatter = (Class<OutputFormatter>) Class.forName(type_formatter_classes[1]);
                OutputFormatter formatterInstance;

				formatterInstance = formatter.getDeclaredConstructor().newInstance();
                Map<Class<CwmsDTO>,OutputFormatter> tmp = new HashMap<>();

                for(String clazz: type_formatter_classes[2].split(";") ){
                    @SuppressWarnings("unchecked")
                    Class<CwmsDTO> formatForClass = (Class<CwmsDTO>)Class.forName(clazz);
                    tmp.put( formatForClass, formatterInstance);
                }

                formatters.put(type,tmp);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				throw new IOException("Failed to load format list, formatter for " + type_formatter_classes[0] + " point to a class with an invalid constructor",e);
			} catch (ClassNotFoundException e) {
				throw new IOException("Failed to find class referenced for formatter " + type_formatter_classes[0],e );
			}
        }

    }


    private String getFormatted(ContentType type, CwmsDTO toFormat) throws FormattingException{
        Objects.requireNonNull(toFormat,"Object to be formatted should not be null");
        for(ContentType key: formatters.keySet()){
            logger.info(key.toString());
        }
        Map<Class<CwmsDTO>, OutputFormatter> contentFormatters = (Map<Class<CwmsDTO>, OutputFormatter>) formatters.get(type);
        if( contentFormatters != null ){
            return contentFormatters.get(toFormat.getClass()).format(toFormat);
        } else {
            throw new FormattingException("No Format for this content-type and data-type : (" + type.toString() + ", " + toFormat.getClass().getName() + ")");
        }

    }

    private String getFormatted(ContentType type, List<? extends CwmsDTO> toFormat) throws FormattingException{
        for(ContentType key: formatters.keySet()){
            logger.info(key.toString());
        }
        Map<Class<CwmsDTO>, OutputFormatter> contentFormatters = (Map<Class<CwmsDTO>, OutputFormatter>) formatters.get(type);
        if( contentFormatters != null ){
            return contentFormatters.get(toFormat.get(0).getClass()).format(toFormat);
        } else {
            throw new FormattingException("No Format for this content-type and data type : (" + type.toString() + ", " + toFormat.getClass().getName() + ")");
        }

    }

    private static void init(){
        if( formats == null ){
            logger.info("creating instance");
            try {
                formats = new Formats();
            } catch( IOException err){
                throw new FormattingException("Failed to load format map", err);
            }
        }
    }

    public static String format(ContentType type, CwmsDTO toFormat) throws FormattingException{
        logger.info("formats");
        init();
        return formats.getFormatted(type,toFormat);
    }

    public static String format(ContentType type, List<? extends CwmsDTO> toFormat) throws FormattingException{
        logger.info("format list");
        init();
        return formats.getFormatted(type,toFormat);
    }


    /**
     * Given the history of RADAR, this function allows the old way to mix with the new way.
     * @param header Accept header value
     * @param queryParam format query parameter value
     * @return an appropriate standard mimetype for lookup
     */
    public static ContentType parseHeaderAndQueryParm(String header, String queryParam){
        if( queryParam != null && !queryParam.isEmpty() ){
            String val = type_map.get(queryParam);
            if( val != null ){
                return new ContentType(val);
            } else {
                throw new FormattingException("content-type " + queryParam + " is not implemented");
            }
        } else if( header == null ){
            throw new FormattingException("no content type or format specified");
        } else {
            String all[] = header.split(",");
            ArrayList<ContentType> contentTypes = new ArrayList<>();
            logger.info("Finding handlers " + all.length);
            for( String ct: all){
                logger.info(ct);
                contentTypes.add(new ContentType(ct));
            }
            Collections.sort(contentTypes);
            logger.info("have " + contentTypes.size());
            for( ContentType ct: contentTypes ){
                logger.info("checking " + ct.toString());
                if( contentTypeList.contains(ct)){
                    return ct;
                }
            }
            for( ContentType ct: contentTypes ){
                if( ct.getType().equals("*/*")){
                    return new ContentType(Formats.JSON);
                }
            }
        }
        throw new FormattingException("Content-Type " + header + " is not available");
    }

}
