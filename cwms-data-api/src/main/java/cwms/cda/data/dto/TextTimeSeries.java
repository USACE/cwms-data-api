package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;

public class TextTimeSeries extends CwmsDTOPaginated {
    private String description;
    private String id;
    private String officeId;


    public TextTimeSeries() {
        super(null);
        id = null;
        description = null;
    }

    public TextTimeSeries(String id, String officeId, String description) {
        super(officeId);
        this.id = id;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }



    @Override
    public void validate() throws FieldException {

    }
}
