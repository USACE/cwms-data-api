package cwms.radar.formatters.csv;

import java.util.List;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    name = "Office CSV",
    description = "Single Office or List of Offices in comma seperated format",
    example = 
    "#Office Name,Long Name,Office Type,Reports To Office\r\n"+
    "CERL,Construction Engineering Research Laboratory,Field Operating Activity	ERD\r\n"+
    "CHL,Coastal and Hydraulics Laboratory,Field Operating Activity	ERD\r\n" +
    "NAB,Baltimore District,District,NAD\r\n"+
    "NAD,North Atlantic Division,Division Headquarters,HQ"
)
public class CsvV1Office implements OutputFormatter{
    
    public String Office;    
    public String longName;    
    public String officeType;    
    public String reportsToOffice;

    @Schema(hidden = true)
    @Override
    public String getContentType() {        
        return Formats.CSV;
    }

    @Override
    public String format(CwmsDTO dto) {
        Office office = (Office)dto;
        StringBuilder builder = new StringBuilder();
        builder.append(getOfficeTabHeader()).append("\r\n");
        builder.append(officeRow(office));
        
        return builder.toString();
    }

    @Override
    @SuppressWarnings("unchecked") // for the daoList conversion
    public String format(List<? extends CwmsDTO> dtoList) {        
        List<Office> offices = (List<Office>)dtoList;
        StringBuilder builder = new StringBuilder();
        builder.append(getOfficeTabHeader()).append("\r\n");
        for( Office office: offices){
            builder.append(officeRow(office)).append("\r\n");
        }
        return builder.toString();
    }

    private String getOfficeTabHeader(){
        return "#Office Name,Long Name,Office Type,Reports To Office";
    }

    private String officeRow(Office office){
        StringBuilder builder = new StringBuilder();
        builder.append(office.getName()).append(",")
               .append(office.getLong_Name()).append(",")
               .append(office.getType()).append(",")
               .append(office.getReports_To());
        return builder.toString();
    }
}
