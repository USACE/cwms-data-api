package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.math.BigInteger;

@JsonDeserialize(builder = SeasonalValueBean.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlAccessorType(XmlAccessType.FIELD)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class SeasonalValueBean {
    @XmlElement(name = "value")
    private Double value;
    @XmlElement(name = "offset-months")
    private Integer offsetMonths;
    @XmlElement(name = "offset-minutes")
    private BigInteger offsetMinutes;

    private SeasonalValueBean() {
        this(new Builder((Double) null));
    }

    private SeasonalValueBean(Builder builder) {
        this.value = builder.value;
        this.offsetMinutes = builder.offsetMinutes;
        this.offsetMonths = builder.offsetMonths;
    }

    public Double getValue()
    {
        return value;
    }
    public BigInteger getOffsetMinutes()
    {
        return offsetMinutes;
    }
    public Integer getOffsetMonths()
    {
        return offsetMonths;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private final Double value;
        private Integer offsetMonths;
        private BigInteger offsetMinutes;


        public Builder(@JsonProperty(value = "value") Double value) {
            this.value = value;
            this.offsetMonths = null;
            this.offsetMinutes = null;
        }

        @JsonCreator
        public Builder(@JsonProperty(value = "value") String value) {
            this.value = Double.valueOf(value);
            this.offsetMonths = null;
            this.offsetMinutes = null;
        }

        public Builder(SeasonalValueBean bean) {
            this.value = bean.getValue();
            this.offsetMonths = bean.getOffsetMonths();
            this.offsetMinutes = bean.getOffsetMinutes();
        }

        public Builder withOffsetMinutes(BigInteger totalOffsetMinutes) {
            offsetMinutes = totalOffsetMinutes;
            return this;
        }

        @JsonProperty(value = "offset-months")
        public Builder withOffsetMonths(Integer totalOffsetMonths) {
            offsetMonths = totalOffsetMonths;
            return this;
        }


        @JsonIgnore
        public Builder withOffsetMonths(Byte totalOffsetMonths) {
            if (totalOffsetMonths != null) {
                offsetMonths = totalOffsetMonths.intValue();
            } else {
                offsetMonths = null;
            }
            return this;
        }

        public SeasonalValueBean build() {
            return new SeasonalValueBean(this);
        }

    }

}
