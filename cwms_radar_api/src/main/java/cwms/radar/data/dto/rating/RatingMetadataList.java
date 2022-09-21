package cwms.radar.data.dto.rating;

import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.formatters.xml.adapters.ZonedDateTimeAdapter;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.Nullable;

public class RatingMetadataList extends CwmsDTOPaginated {

    private List<RatingMetadata> metadata;

    private RatingMetadataList() {

    }


    private RatingMetadataList(RatingMetadataList.Builder builder) {
        super(builder.buildPage(), builder.pageSize);
        this.metadata = builder.metadata;
        this.nextPage = builder.buildNextPage();
    }

    public List<RatingMetadata> getRatingMetadata() {
        return Collections.unmodifiableList(metadata);
    }

    public static class KeySet {
        String specOfficeId;
        String specTemplateId;
        String specRatingId;
        String ratingOfficeId;
        String ratingRatingId;
        ZonedDateTime ratingEffectiveDate;

        public KeySet(String specOfficeId, String specTemplateId, String specRatingId,
                      String ratingOfficeId, String ratingRatingId,
                      ZonedDateTime ratingEffectiveDate) {
            this.specOfficeId = specOfficeId;
            this.specTemplateId = specTemplateId;
            this.specRatingId = specRatingId;
            this.ratingOfficeId = ratingOfficeId;
            this.ratingRatingId = ratingRatingId;
            this.ratingEffectiveDate = ratingEffectiveDate;
        }

        public static KeySet build(Object[] parts) {
            KeySet retval = null;

            if (parts != null && parts.length >= 6) {
                Object part = parts[5];
                ZonedDateTime effective = asZonedDateTime(part);
                retval = new KeySet((String) parts[0], (String) parts[1], (String) parts[2],
                        (String) parts[3], (String) parts[4], effective);
            }
            return retval;
        }


        public static Integer parsePageSize(Object[] parts, int pageSize) {
            Integer retval = pageSize;
            if (parts != null && parts.length >= 7) {
                retval = Integer.parseInt((String) parts[6]);
            }
            return retval;
        }

        public Object[] getSeekValues() {
            if (specOfficeId == null && specTemplateId == null && specRatingId == null
                    && ratingOfficeId == null && ratingRatingId == null && ratingEffectiveDate == null) {
                // Do we return null or empty?
                return new Object[]{};
            }
            return new Object[]{specOfficeId, specTemplateId, specRatingId,
                    ratingOfficeId, ratingRatingId, ratingEffectiveDate
            };
        }

        @Nullable
        public static ZonedDateTime asZonedDateTime(Object part) {
            ZonedDateTime effective = null;
            if (part instanceof String) {
                ZonedDateTimeAdapter adapter = new ZonedDateTimeAdapter();
                try {
                    effective = adapter.unmarshal((String) part);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else if (part instanceof ZonedDateTime) {
                effective = (ZonedDateTime) part;
            }
            return effective;
        }

        public static String toString(ZonedDateTime effectiveDate) {
            String effectiveDateStr = null;
            if (effectiveDate != null) {
                ZonedDateTimeAdapter adapter = new ZonedDateTimeAdapter();
                try {
                    effectiveDateStr = adapter.marshal(effectiveDate);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return effectiveDateStr;
        }
    }


    @Override
    public void validate() throws FieldException {

    }

    public static class Builder {

        private final int pageSize;

        private List<RatingMetadata> metadata;

        private KeySet keySet;

        public Builder(int pageSize) {
            this.pageSize = pageSize;
        }

        public Builder withMetadata(Collection<RatingMetadata> col) {
            this.metadata = new ArrayList<>(col);
            return this;
        }

        public RatingMetadataList build() {
            RatingMetadataList retval = new RatingMetadataList(this);

            return retval;
        }

        public KeySet buildNextKeySet() {
            KeySet retval = null;

            RatingMetadata last = null;
            if (metadata != null && !metadata.isEmpty()) {
                last = metadata.get(metadata.size() - 1);
            }

            if (last != null) {
                List<AbstractRatingMetadata> ratings = last.getRatings();
                AbstractRatingMetadata lastRating = ratings.get(ratings.size() - 1);

                ZonedDateTime effectiveDate = lastRating.getEffectiveDate();

                RatingSpec spec = last.getRatingSpec();
                retval = new KeySet(spec.getOfficeId(), spec.getTemplateId(), spec.getRatingId(),
                        lastRating.getOfficeId(), lastRating.getRatingSpecId(), effectiveDate);
            }

            return retval;
        }


        public String buildPage() {
            String retval = "";
            // This method needs to return the "page" cursor for this class
            // suitable for passing to the CwmsDTOPaginated constructor
            // CwmsDTOPaginated always puts the pageSize as the last entry in the list so
            // we don't need to include it in these results.


            if (keySet != null) {
                Object[] seekValues = keySet.getSeekValues();

                for (int i = 0; i < seekValues.length; i++) {
                    if (seekValues[i] instanceof ZonedDateTime) {
                        seekValues[i] = KeySet.toString((ZonedDateTime) seekValues[i]);
                    }
                }

                retval = CwmsDTOPaginated.encodeCursor(seekValues);
            }
            return retval;
        }

        public String buildNextPage() {
            String retval = "";

            KeySet next = buildNextKeySet();
            if (next != null) {
                Object[] seekValues = next.getSeekValues();

                for (int i = 0; i < seekValues.length; i++) {
                    if (seekValues[i] instanceof ZonedDateTime) {
                        seekValues[i] = KeySet.toString((ZonedDateTime) seekValues[i]);
                    }
                }
                retval = CwmsDTOPaginated.encodeCursor(seekValues);
            }
            return retval;
        }

        public Builder withKeySet(KeySet keySet) {
            this.keySet = keySet;
            return this;
        }
    }

}

