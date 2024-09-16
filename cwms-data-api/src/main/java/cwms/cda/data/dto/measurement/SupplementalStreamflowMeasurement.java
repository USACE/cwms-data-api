package cwms.cda.data.dto.measurement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = SupplementalStreamflowMeasurement.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class SupplementalStreamflowMeasurement extends CwmsDTOBase
{

    private final Double channelFlow;
    private final Double overbankFlow;
    private final Double overbankMaxDepth;
    private final Double channelMaxDepth;
    private final Double avgVelocity;
    private final Double surfaceVelocity;
    private final Double maxVelocity;
    private final Double effectiveFlowArea;
    private final Double crossSectionalArea;
    private final Double meanGage;
    private final Double topWidth;
    private final Double mainChannelArea;
    private final Double overbankArea;

    private SupplementalStreamflowMeasurement(Builder builder)
    {
        this.channelFlow = builder.channelFlow;
        this.overbankFlow = builder.overbankFlow;
        this.overbankMaxDepth = builder.overbankMaxDepth;
        this.channelMaxDepth = builder.channelMaxDepth;
        this.avgVelocity = builder.avgVelocity;
        this.surfaceVelocity = builder.surfaceVelocity;
        this.maxVelocity = builder.maxVelocity;
        this.effectiveFlowArea = builder.effectiveFlowArea;
        this.crossSectionalArea = builder.crossSectionalArea;
        this.meanGage = builder.meanGage;
        this.topWidth = builder.topWidth;
        this.mainChannelArea = builder.mainChannelArea;
        this.overbankArea = builder.overbankArea;
    }

    public Double getChannelFlow()
    {
        return channelFlow;
    }

    public Double getOverbankFlow()
    {
        return overbankFlow;
    }

    public Double getOverbankMaxDepth()
    {
        return overbankMaxDepth;
    }

    public Double getChannelMaxDepth()
    {
        return channelMaxDepth;
    }

    public Double getAvgVelocity()
    {
        return avgVelocity;
    }

    public Double getSurfaceVelocity()
    {
        return surfaceVelocity;
    }

    public Double getMaxVelocity()
    {
        return maxVelocity;
    }

    public Double getEffectiveFlowArea()
    {
        return effectiveFlowArea;
    }

    public Double getCrossSectionalArea()
    {
        return crossSectionalArea;
    }

    public Double getMeanGage()
    {
        return meanGage;
    }

    public Double getTopWidth()
    {
        return topWidth;
    }

    public Double getMainChannelArea()
    {
        return mainChannelArea;
    }

    public Double getOverbankArea()
    {
        return overbankArea;
    }

    public static final class Builder
    {
        private Double channelFlow;
        private Double overbankFlow;
        private Double overbankMaxDepth;
        private Double channelMaxDepth;
        private Double avgVelocity;
        private Double surfaceVelocity;
        private Double maxVelocity;
        private Double effectiveFlowArea;
        private Double crossSectionalArea;
        private Double meanGage;
        private Double topWidth;
        private Double mainChannelArea;
        private Double overbankArea;

        public Builder withChannelFlow(Double channelFlow)
        {
            this.channelFlow = channelFlow;
            return this;
        }

        public Builder withOverbankFlow(Double overbankFlow)
        {
            this.overbankFlow = overbankFlow;
            return this;
        }

        public Builder withOverbankMaxDepth(Double overbankMaxDepth)
        {
            this.overbankMaxDepth = overbankMaxDepth;
            return this;
        }

        public Builder withChannelMaxDepth(Double channelMaxDepth)
        {
            this.channelMaxDepth = channelMaxDepth;
            return this;
        }

        public Builder withAvgVelocity(Double avgVelocity)
        {
            this.avgVelocity = avgVelocity;
            return this;
        }

        public Builder withSurfaceVelocity(Double surfaceVelocity)
        {
            this.surfaceVelocity = surfaceVelocity;
            return this;
        }

        public Builder withMaxVelocity(Double maxVelocity)
        {
            this.maxVelocity = maxVelocity;
            return this;
        }

        public Builder withEffectiveFlowArea(Double effectiveFlowArea)
        {
            this.effectiveFlowArea = effectiveFlowArea;
            return this;
        }

        public Builder withCrossSectionalArea(Double crossSectionalArea)
        {
            this.crossSectionalArea = crossSectionalArea;
            return this;
        }

        public Builder withMeanGage(Double meanGage)
        {
            this.meanGage = meanGage;
            return this;
        }

        public Builder withTopWidth(Double topWidth)
        {
            this.topWidth = topWidth;
            return this;
        }

        public Builder withMainChannelArea(Double mainChannelArea)
        {
            this.mainChannelArea = mainChannelArea;
            return this;
        }

        public Builder withOverbankArea(Double overbankArea)
        {
            this.overbankArea = overbankArea;
            return this;
        }

        public SupplementalStreamflowMeasurement build()
        {
            return new SupplementalStreamflowMeasurement(this);
        }
    }
}
