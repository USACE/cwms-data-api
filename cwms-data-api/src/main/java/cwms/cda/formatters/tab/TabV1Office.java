package cwms.cda.formatters.tab;

import java.util.List;

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    name = "Office_Tabulation",
    description = "Single Office or List of Offices in tab separated format",
    example = 
    "#Office Name<tab>Long Name<tab>Office Type<tab>Reports To Office\r\n"+
    "CERL	Construction Engineering Research Laboratory	Field Operating Activity	ERD\r\n"+
    "CHL	Coastal and Hydraulics Laboratory	Field Operating Activity	ERD\r\nNAB	Baltimore District	District	NAD"+
    "NAD	North Atlantic Division	Division Headquarters	HQ"
)
public class TabV1Office implements OutputFormatter {

    public String Office;
    public String longName;
    public String officeType;
    public String reportsToOffice;

    @Schema(hidden = true)
    @Override
    public String getContentType() {
        return Formats.TAB;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        Office office = (Office)dto;
        StringBuilder builder = new StringBuilder();
        builder.append(getOfficeTabHeader()).append("\r\n");
        builder.append(officeRow(office));

        return builder.toString();
    }

    @Override
    @SuppressWarnings("unchecked") // for the daoList conversion
    public String format(List<? extends CwmsDTOBase> dtoList) {
        List<Office> offices = (List<Office>)dtoList;
        StringBuilder builder = new StringBuilder();
        builder.append(getOfficeTabHeader()).append("\r\n");
        for (Office office: offices) {
            builder.append(officeRow(office)).append("\r\n");
        }
        return builder.toString();
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        throw new UnsupportedOperationException("Unable to process your request. Deserialization of "
                + getContentType() + " not yet supported.");
    }

    private String getOfficeTabHeader() {
        return "#Office Name	Long Name	Office Type	Reports To Office";
    }

    private String officeRow(Office office){
        StringBuilder builder = new StringBuilder();
        builder.append(office.getName()).append("\t")
               .append(office.getLongName()).append("\t")
               .append(office.getType()).append("\t")
               .append(office.getReportsTo());
        return builder.toString();
    }
}
