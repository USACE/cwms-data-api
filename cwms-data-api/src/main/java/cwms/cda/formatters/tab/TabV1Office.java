package cwms.cda.formatters.tab;


import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

@Schema(
    name = "Office_Tabulation",
    description = "Single Office or List of Offices in tab separated format",
    example = 
    "#Office Name<tab>Long Name<tab>Office Type<tab>Reports To Office\r\n"
    + "CERL\tConstruction Engineering Research Laboratory\tField Operating Activity\tERD\r\n"
    + "CHL\tCoastal and Hydraulics Laboratory\tField Operating Activity\tERD\r\nNAB\tBaltimore District\tDistrict\tNAD"
    + "NAD\tNorth Atlantic Division\tDivision Headquarters\tHQ"
)
public class TabV1Office implements OutputFormatter {

    public String office;
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

    private String getOfficeTabHeader() {
        return "#Office Name\tLong Name\tOffice Type\tReports To Office";
    }

    private String officeRow(Office office) {
        StringBuilder builder = new StringBuilder();
        builder.append(office.getName()).append("\t")
               .append(office.getLongName()).append("\t")
               .append(office.getType()).append("\t")
               .append(office.getReportsTo());
        return builder.toString();
    }
}
