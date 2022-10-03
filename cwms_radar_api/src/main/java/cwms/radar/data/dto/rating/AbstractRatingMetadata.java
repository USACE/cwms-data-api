package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonFormat;
import cwms.radar.data.dto.VerticalDatumInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.ZonedDateTime;

@Schema(
        description = "Rating Metadata",
        oneOf = {
                TableRating.class,
                TransitionalRating.class,
                VirtualRating.class,
                ExpressionRating.class,
                UsgsStreamRating.class
        },
        discriminatorProperty = "ratingType"
)
public abstract class AbstractRatingMetadata {
    // This is the "discriminator" field to (hopefully) make swagger work

    private final String ratingType;
    private final String officeId;

    private final String ratingSpecId;


    private final String unitsId;

    private final boolean active;


    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final ZonedDateTime effectiveDate;


    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final ZonedDateTime createDate;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final ZonedDateTime transitionDate;

    private final String description;

    private final VerticalDatumInfo verticalDatumInfo;

    protected AbstractRatingMetadata(Builder builder) {
        this.officeId = builder.officeId;
        this.ratingType = builder.ratingType;
        this.unitsId = builder.unitsId;
        this.active = builder.active;
        this.effectiveDate = builder.effectiveDate;
        this.createDate = builder.createDate;
        this.transitionDate = builder.transitionDate;
        this.description = builder.description;
        this.verticalDatumInfo = builder.verticalDatumInfo;
        this.ratingSpecId = builder.ratingSpecId;

    }

    public String getOfficeId() {
        return officeId;
    }

    public String getRatingSpecId() {
        return ratingSpecId;
    }

    public String getUnitsId() {
        return unitsId;
    }

    public boolean isActive() {
        return active;
    }

    public ZonedDateTime getEffectiveDate() {
        return effectiveDate;
    }

    public ZonedDateTime getCreateDate() {
        return createDate;
    }

    public ZonedDateTime getTransitionDate() {
        return transitionDate;
    }

    public String getDescription() {
        return description;
    }

    public String getRatingType() {
        return ratingType;
    }

    public VerticalDatumInfo getVerticalDatumInfo() {
        return verticalDatumInfo;
    }

    public abstract static class Builder {
        private VerticalDatumInfo verticalDatumInfo;
        private String officeId;
        private String ratingSpecId;
        protected String ratingType;

        private String unitsId;

        private boolean active;

        private ZonedDateTime effectiveDate;

        private ZonedDateTime createDate;

        private ZonedDateTime transitionDate;

        private String description;

        protected Builder() {
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withRatingSpecId(String ratingSpecId) {
            this.ratingSpecId = ratingSpecId;
            return this;
        }

        public Builder withRatingType(String ratingType) {
            this.ratingType = ratingType;
            return this;
        }

        public Builder withUnitsId(String unitsId) {
            this.unitsId = unitsId;
            return this;
        }

        public Builder withActive(boolean active) {
            this.active = active;
            return this;
        }

        public Builder withEffectiveDate(ZonedDateTime effectiveDate) {
            this.effectiveDate = effectiveDate;
            return this;
        }

        public Builder withCreateDate(ZonedDateTime createDate) {
            this.createDate = createDate;
            return this;
        }

        public Builder withTransitionDate(ZonedDateTime transitionDate) {
            this.transitionDate = transitionDate;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withVerticalDatumInfo(VerticalDatumInfo verticalDatumInfo) {
            this.verticalDatumInfo = verticalDatumInfo;
            return this;
        }

        public Builder withFields(Builder builder) {
            this.officeId = builder.officeId;
            this.ratingSpecId = builder.ratingSpecId;
            this.ratingType = builder.ratingType;
            this.unitsId = builder.unitsId;
            this.active = builder.active;
            this.effectiveDate = builder.effectiveDate;
            this.createDate = builder.createDate;
            this.transitionDate = builder.transitionDate;
            this.description = builder.description;
            this.verticalDatumInfo = builder.verticalDatumInfo;
            return this;
        }


        public abstract AbstractRatingMetadata build();
    }

}
