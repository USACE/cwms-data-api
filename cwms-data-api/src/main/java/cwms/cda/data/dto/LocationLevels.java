package cwms.cda.data.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

@XmlRootElement(name = "location-levels")
@XmlSeeAlso(LocationLevel.class)
@XmlAccessorType(XmlAccessType.FIELD)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class LocationLevels extends CwmsDTOPaginated {
    @XmlElementWrapper
    @XmlElement(name = "location-level")
    @Schema(description = "List of retrieved location levels")
    private List<LocationLevel> levels;

    @SuppressWarnings("unused") // for JAXB to handle marshalling
    private LocationLevels() {
    }

    private int offset;

    public LocationLevels(int offset, int pageSize, Integer total) {
        super(Integer.toString(offset), pageSize, total);
        levels = new ArrayList<>();
        this.offset = offset;
    }

    public List<LocationLevel> getLevels() {
        return Collections.unmodifiableList(levels);
    }


    public static class Builder {
        private LocationLevels workingLevels;

        public Builder(int offset, int pageSize, Integer total) {
            workingLevels = new LocationLevels(offset, pageSize, total);
        }

        public LocationLevels build() {
            if (this.workingLevels.levels.size() == this.workingLevels.pageSize) {

                String cursor =
                        Integer.toString(this.workingLevels.offset + this.workingLevels.levels.size());
                this.workingLevels.nextPage = encodeCursor(cursor,
                        this.workingLevels.pageSize,
                        this.workingLevels.total);
            } else {
                this.workingLevels.nextPage = null;
            }
            return workingLevels;
        }

        public LocationLevels.Builder add(LocationLevel level) {
            this.workingLevels.levels.add(level);
            return this;
        }

        public LocationLevels.Builder addAll(Collection<? extends LocationLevel> levels) {
            this.workingLevels.levels.addAll(levels);
            return this;
        }
    }


    @Override
    public void validate() throws FieldException {


    }

}
