package cwms.radar.data.dao;

import cwms.radar.data.dto.rating.AbstractRatingMetadata;
import cwms.radar.data.dto.rating.ExpressionRating;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.TableRating;
import cwms.radar.data.dto.rating.TransitionalRating;
import cwms.radar.data.dto.rating.UsgsStreamRating;
import cwms.radar.data.dto.rating.VirtualRating;
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
        RatingSpec retval = null;

        if (spec != null) {
            return new RatingSpec.Builder()
                    .fromRatingSpec(spec).build();
        }

        return retval;
    }

    @Nullable
    public static Set<AbstractRatingMetadata> toDTO(Set<AbstractRating> ratings) {
        Set<AbstractRatingMetadata> retval = null;
        if (ratings != null) {
            retval = new LinkedHashSet<>();
            for (AbstractRating rating : ratings) {
                retval.add(toDTO(rating));
            }
        }
        return retval;
    }

    private static AbstractRatingMetadata toDTO(AbstractRating rating) {
        AbstractRatingMetadata retval = null;

        if (rating instanceof UsgsStreamTableRating) {
            retval = toUsgsStream((UsgsStreamTableRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.VirtualRating) {
            retval = toVirtual((hec.data.cwmsRating.VirtualRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.ExpressionRating) {
            retval = toExpression((hec.data.cwmsRating.ExpressionRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.TransitionalRating) {
            retval = toTransitional((hec.data.cwmsRating.TransitionalRating) rating);
        } else if (rating instanceof hec.data.cwmsRating.TableRating) {
            retval = toTable((hec.data.cwmsRating.TableRating) rating);
        }

        return retval;
    }

    private static UsgsStreamRating toUsgsStream(UsgsStreamTableRating usgs) {
        UsgsStreamRating retval = null;
        if (usgs != null) {
            UsgsStreamRating.Builder builder = new UsgsStreamRating.Builder();
            withAbstractFields(builder, usgs);

            retval = builder.build();
        }
        return retval;
    }

    private static VirtualRating toVirtual(hec.data.cwmsRating.VirtualRating rating) {
        VirtualRating retval = null;
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
            retval = builder.build();
        }
        return retval;
    }

    private static TransitionalRating toTransitional(
            hec.data.cwmsRating.TransitionalRating rating) {
        TransitionalRating retval = null;
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

            retval = builder.build();
        }
        return retval;
    }

    private static ExpressionRating toExpression(hec.data.cwmsRating.ExpressionRating rating) {
        ExpressionRating retval = null;
        if (rating != null) {
            ExpressionRating.Builder builder = new ExpressionRating.Builder();
            withAbstractFields(builder, rating);

            builder.withExpression(rating.getExpression());

            retval = builder.build();
        }
        return retval;
    }

    private static TableRating toTable(hec.data.cwmsRating.TableRating rating) {
        TableRating retval = null;
        if (rating != null) {
            TableRating.Builder builder = new TableRating.Builder();
            withAbstractFields(builder, rating);

            retval = builder.build();
        }
        return retval;
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
