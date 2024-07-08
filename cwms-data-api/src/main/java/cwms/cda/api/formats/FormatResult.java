package cwms.cda.api.formats;

public class FormatResult {
    private String result;
    private String contentType;

    public FormatResult(String result, String contentType) {
        this.result = result;
        this.contentType = contentType;
    }

    public int length() {
        return result.length();
    }

    /**
     * @return String return the result
     */
    @Override
    public String toString() {
        return result;
    }


    /**
     * @return String return the contentType
     */
    public String getContentType() {
        return contentType;
    }


}
