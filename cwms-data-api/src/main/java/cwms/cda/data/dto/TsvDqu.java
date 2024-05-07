package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import java.text.MessageFormat;
import java.util.Date;

@JsonDeserialize(builder = TsvDqu.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TsvDqu extends CwmsDTO {

    private final String cwmsTsId;
    private final String unitId;
    private final Date dateTime;
    private final Date versionDate;
    private final Date dataEntryDate;
    private final Double value;
    private final Long qualityCode;
    private final Date startDate;
    private final Date endDate;

    private TsvDqu(Builder builder) {
        super(builder.officeId);

        this.cwmsTsId = builder.cwmsTsId;
        this.unitId = builder.unitId;
        this.dateTime = builder.dateTime;
        this.versionDate = builder.versionDate;
        this.dataEntryDate = builder.dataEntryDate;
        this.value = builder.value;
        this.qualityCode = builder.qualityCode;
        this.startDate = builder.startDate;
        this.endDate = builder.endDate;
    }

    public String getUnitId() {
        return this.unitId;
    }

    public Date getDateTime() {
        return this.dateTime;
    }


    public String getCwmsTsId() {
        return this.cwmsTsId;
    }

    public Date getVersionDate() {
        return this.versionDate;
    }

    public Date getDataEntryDate() {
        return this.dataEntryDate;
    }

    public Double getValue() {
        return this.value;
    }

    public Long getQualityCode() {
        return this.qualityCode;
    }

    public Date getStartDate() {
        return this.startDate;
    }

    public Date getEndDate() {
        return this.endDate;
    }

    @Override
    public String toString() {
        return MessageFormat.format("TsvDqu'{'"
                + "officeId={0}, cwmsTsId={1}, unitId={2}, dateTime={3}, versionDate={4}, "
                        + "dataEntryDate={5}, value={6}, qualityCode={7}, startDate={8}, endDate={9}'}'",
                officeId, cwmsTsId, unitId, dateTime, versionDate,
                dataEntryDate, value, qualityCode, startDate, endDate);
    }

    @Override
    public void validate() throws FieldException {

    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String officeId;
        private String cwmsTsId;
        private String unitId;
        private Date dateTime;
        private Date versionDate;
        private Date dataEntryDate;
        private Double value;
        private Long qualityCode;
        private Date startDate;
        private Date endDate;

        public Builder() {
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withCwmsTsId(String cwmsTsId) {
            this.cwmsTsId = cwmsTsId;
            return this;
        }

        public Builder withUnitId(String unitId) {
            this.unitId = unitId;
            return this;
        }

        public Builder withDateTime(Date dateTime) {
            this.dateTime = dateTime;
            return this;
        }

        public Builder withVersionDate(Date versionDate) {
            this.versionDate = versionDate;
            return this;
        }

        public Builder withDataEntryDate(Date dataEntryDate) {
            this.dataEntryDate = dataEntryDate;
            return this;
        }

        public Builder withValue(Double value) {
            this.value = value;
            return this;
        }

        public Builder withQualityCode(Long qualityCode) {
            this.qualityCode = qualityCode;
            return this;
        }

        public Builder withStartDate(Date startDate) {
            this.startDate = startDate;
            return this;
        }

        public Builder withEndDate(Date endDate) {
            this.endDate = endDate;
            return this;
        }

        public Builder from(TsvDqu other) {
            if (other == null) {
                return withOfficeId(null)
                        .withCwmsTsId(null)
                        .withUnitId(null)
                        .withDateTime(null)
                        .withVersionDate(null)
                        .withDataEntryDate(null)
                        .withValue(null)
                        .withQualityCode(null)
                        .withStartDate(null)
                        .withEndDate(null);
            } else {
                return withOfficeId(other.officeId)
                        .withCwmsTsId(other.cwmsTsId)
                        .withUnitId(other.unitId)
                        .withDateTime(other.dateTime)
                        .withVersionDate(other.versionDate)
                        .withDataEntryDate(other.dataEntryDate)
                        .withValue(other.value)
                        .withQualityCode(other.qualityCode)
                        .withStartDate(other.startDate)
                        .withEndDate(other.endDate);
            }
        }

        public TsvDqu build() {
            return new TsvDqu(this);
        }
    }
}
