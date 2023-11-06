package cwms.cda.data.dao;

import cwms.cda.data.dto.rating.AbstractRatingMetadata;
import cwms.cda.data.dto.rating.ExpressionRating;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.data.dto.rating.TableRating;
import cwms.cda.data.dto.rating.TransitionalRating;
import cwms.cda.data.dto.rating.UsgsStreamRating;
import cwms.cda.data.dto.rating.VirtualRating;
import hec.data.cwmsRating.AbstractRating;
import hec.data.cwmsRating.SourceRating;
import hec.data.cwmsRating.UsgsStreamTableRating;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

public class RatingAdapter {

    private RatingAdapter() {
    }

    private static RatingSpec toDTO(hec.data.cwmsRating.RatingSpec spec) {
        RatingSpec retVal = null;

        if (spec != null) {
            return new RatingSpec.Builder()
                    .fromRatingSpec(spec).build();
        }

        return retVal;
    }

    @Nullable
    public static Set<AbstractRatingMetadata> toDTO(Set<AbstractRating> ratings) {
        Set<AbstractRatingMetadata> retVal = null;
        if (ratings != null) {
            retVal = new LinkedHashSet<>();
            for (AbstractRating rating : ratings) {
                retVal.add(toDTO(rating));
            }
        }
        return retVal;
    }

    private static AbstractRatingMetadata toDTO(AbstractRating rating) {
        AbstractRatingMetadata retVal = null;

        if (rating instanceof UsgsStreamTableRating) {
            retVal = toUsgsStream((UsgsStreamTableRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.VirtualRating) {
            retVal = toVirtual((hec.data.cwmsRating.VirtualRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.ExpressionRating) {
            retVal = toExpression((hec.data.cwmsRating.ExpressionRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.TransitionalRating) {
            retVal = toTransitional((hec.data.cwmsRating.TransitionalRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.TableRating) {
            retVal = toTable((hec.data.cwmsRating.TableRating) rating);
        }

        return retVal;
    }

    private static UsgsStreamRating toUsgsStream(UsgsStreamTableRating usgs) {
        UsgsStreamRating retVal = null;
        if (usgs != null) {
            UsgsStreamRating.Builder builder = new UsgsStreamRating.Builder();
            withAbstractFields(builder, usgs);

            retVal = builder.build();
        }
        return retVal;
    }

    private static VirtualRating toVirtual(hec.data.cwmsRating.VirtualRating rating) {
        VirtualRating retVal = null;
        if (rating != null) {
            VirtualRating.Builder builder = new VirtualRating.Builder();
            withAbstractFields(builder, rating);

            builder.withConnections(rating.getConnections());

            // Not sure exactly how to handle SourceRating at the moment.
//            SourceRating[] sourceRatings = rating.getSourceRatings();
//            for (SourceRating sourceRating : sourceRatings) {
////                VirtualRating.SourceRating sr =
//                      new VirtualRating.SourceRating(sourceRating.getRatingId(),
////                        sourceRating.getRatingType(), sourceRating.getRatingUnits());
////                builder.withSourceRating(sr);
//            }
            retVal = builder.build();
        }
        return retVal;
    }

    private static TransitionalRating toTransitional(
            hec.data.cwmsRating.TransitionalRating rating) {
        TransitionalRating retVal = null;
        if (rating != null) {
            TransitionalRating.Builder builder = new TransitionalRating.Builder();
            withAbstractFields(builder, rating);

            String[] evaluationStrings = rating.getEvaluationStrings();
            builder.withEvaluations(Arrays.asList(evaluationStrings));

            String[] conditionStrings = rating.getConditionStrings();
            builder.withConditions(Arrays.asList(conditionStrings));

            List<String> ratingSpecIds = new ArrayList<>();
            SourceRating[] sourceRatings = rating.getSourceRatings();
            for (SourceRating sourceRating : sourceRatings) {
                ratingSpecIds.add(sourceRating.getRatingSet().getRatingSpec().getRatingSpecId());
            }
            builder.withSourceRatingIds(ratingSpecIds);

            retVal = builder.build();
        }
        return retVal;
    }

    private static ExpressionRating toExpression(hec.data.cwmsRating.ExpressionRating rating) {
        ExpressionRating retVal = null;
        if (rating != null) {
            ExpressionRating.Builder builder = new ExpressionRating.Builder();
            withAbstractFields(builder, rating);

            builder.withExpression(rating.getExpression());

            retVal = builder.build();
        }
        return retVal;
    }

    private static TableRating toTable(hec.data.cwmsRating.TableRating rating) {
        TableRating retVal = null;
        if (rating != null) {
            TableRating.Builder builder = new TableRating.Builder();
            withAbstractFields(builder, rating);

            retVal = builder.build();
        }
        return retVal;
    }


    public static AbstractRatingMetadata.Builder withAbstractFields(
            AbstractRatingMetadata.Builder builder, AbstractRating rating) {

        ZonedDateTime effectiveZdt = null;
        long effectiveDate = rating.getEffectiveDate();
        if (effectiveDate > 0) {
            effectiveZdt = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(effectiveDate),
                    ZoneId.of("UTC"));
        }

        ZonedDateTime createZdt = null;
        long createDate = rating.getCreateDate();
        if (createDate > 0) {
            createZdt = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(createDate),
                    ZoneId.of("UTC"));
        }

        ZonedDateTime transZdt = null;
        long transDate = rating.getTransitionStartDate();
        if (transDate > 0) {
            transZdt = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(transDate),
                    ZoneId.of("UTC"));
        }

        return builder.withOfficeId(rating.getOfficeId())
                .withRatingSpecId(rating.getRatingSpecId())
                .withDescription(rating.getDescription())
                .withUnitsId(rating.getRatingUnitsId())
                .withActive(rating.isActive())
                .withEffectiveDate(effectiveZdt)
                .withCreateDate(createZdt)
                .withTransitionDate(transZdt);
    }


}
