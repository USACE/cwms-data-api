package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonIgnore;
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


    @JsonIgnore
    public int getSize(){
        int retval = 0;
        if(metadata != null){
            for (RatingMetadata ratingMetadata : metadata) {
                retval += ratingMetadata.getSize();
            }
        }
        return retval;
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
        private static final String[] sortFields = new String[]{
                "AV_RATING_SPEC.OFFICE_ID",
                "AV_RATING_SPEC.TEMPLATE_ID",
                "AV_RATING_SPEC.RATING_ID",
                "AV_RATING.EFFECTIVE_DATE"
        };

        String specOfficeId;
        String specTemplateId;
        String specRatingId;

        ZonedDateTime ratingEffectiveDate;

        public KeySet(String specOfficeId, String specTemplateId, String specRatingId,
                      ZonedDateTime ratingEffectiveDate) {
            this.specOfficeId = specOfficeId;
            this.specTemplateId = specTemplateId;
            this.specRatingId = specRatingId;
            this.ratingEffectiveDate = ratingEffectiveDate;
        }

        public static KeySet build(Object[] parts) {
            KeySet retval = null;

            if (parts != null && parts.length >= sortFields.length) {
                Object last = parts[parts.length - 1];
                ZonedDateTime effective = asZonedDateTime(last);
                retval = new KeySet((String) parts[0], (String) parts[1], (String) parts[2],
                        effective);
            }
            return retval;
        }


        // CwmsDTOPaginated puts the pageSize as the last element.
        // We know we need NUM_FIELDS elements so only return the last element as
        // pageSize if they gave us more than NUM_FIELDS elements.
        public static Integer parsePageSize(Object[] parts, int pageSize) {
            Integer retval = pageSize;
            if (parts != null && parts.length > sortFields.length) {
                String lastPart = (String) parts[parts.length - 1];
                retval = Integer.parseInt(lastPart);
            }
            return retval;
        }

        public Object[] getSeekValues() {
            if (specOfficeId == null && specTemplateId == null && specRatingId == null
                    && ratingEffectiveDate == null) {
                // Do we return null or empty?
                return new Object[]{};
            }
            return new Object[]{specOfficeId, specTemplateId, specRatingId, ratingEffectiveDate};
        }

        public static String [] getSeekFieldNames(){
            return sortFields;
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

        private int getSize(){
            int retval = 0;
            if(metadata != null){
                for (RatingMetadata ratingMetadata : metadata) {
                    retval += ratingMetadata.getSize();
                }
            }
            return retval;
        }

        public RatingMetadataList build() {
            return new RatingMetadataList(this);
        }

        public KeySet buildNextKeySet() {
            KeySet retval = null;

            // If we asked for 20 per-page and have 20 there might be another page.
            // If we asked for 20 and got 19 there isn't another page.
            if (getSize() >= pageSize) {

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
                            effectiveDate);
                }
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

