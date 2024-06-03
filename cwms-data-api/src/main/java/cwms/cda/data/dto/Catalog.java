package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dao.CatalogRequestParameters;
import cwms.cda.data.dto.catalog.CatalogEntry;
import cwms.cda.data.dto.catalog.LocationCatalogEntry;
import cwms.cda.data.dto.catalog.TimeseriesCatalogEntry;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "catalog")
@XmlAccessorType(XmlAccessType.FIELD)
@FormattableWith(contentType = Formats.XML, formatter = XMLv1.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class Catalog extends CwmsDTOPaginated {
    @Schema(
            oneOf = {
                    LocationCatalogEntry.class,
                    TimeseriesCatalogEntry.class
            }
    )
    @XmlElementWrapper(name = "entries")
    @XmlElement(name = "entry")
    private List<? extends CatalogEntry> entries;

    @SuppressWarnings("unused") // required so JAXB can initialize and marshal
    private Catalog() {
    }

    public Catalog(String page, int total, int pageSize, List<? extends CatalogEntry> entries) {
        this(page, total, pageSize, entries, new CatalogRequestParameters.Builder().build());
    }


    public Catalog(String page, int total, int pageSize, List<? extends CatalogEntry> entries,
                   CatalogRequestParameters param) {
        super(page, pageSize, total);

        Objects.requireNonNull(entries, "List of catalog entries must be a valid list, even if empty");
        this.entries = entries;
        if (entries.size() == pageSize) {
            nextPage = encodeCursor(new CatalogPage(
                            entries.get(entries.size() - 1).getCursor(),
                            param
                    ).toString(),
                    pageSize, total);

        } else {
            nextPage = null;
        }
    }

    /**
     * @return List<? extends CatalogEntry> return the entries
     */
    public List<? extends CatalogEntry> getEntries() {
        return entries;
    }

    @Override
    public void validate() throws FieldException{
        // catalogs are never accepted as user input
    }

    public static class CatalogPage {
        private final String curOffice;
        private final String cursorId;
        private final String searchOffice;
        private final String idLike;
        private final String locCategoryLike;
        private final String locGroupLike;
        private final String tsCategoryLike;
        private final String tsGroupLike;
        private final String boundingOfficeLike;
        private final boolean includeExtents;
        private final boolean excludeEmpty;
        private int total;
        private int pageSize;

        public CatalogPage(String page) {
            String[] parts = CwmsDTOPaginated.decodeCursor(page, CwmsDTOPaginated.delimiter);

            if (parts.length != 12) {
                throw new IllegalArgumentException("Invalid Catalog Page Provided, please verify "
                        + "you are using a page variable from the catalog endpoint");
            }
            String[] idParts = parts[0].split("/");
            curOffice = idParts[0];  // this is the cursor office
            cursorId = idParts[1];   // this is the cursor id
            searchOffice = nullOrVal(parts[1]);
            idLike = nullOrVal(parts[2]);
            locCategoryLike = nullOrVal(parts[3]);
            locGroupLike = nullOrVal(parts[4]);
            tsCategoryLike = nullOrVal(parts[5]);
            tsGroupLike = nullOrVal(parts[6]);
            boundingOfficeLike = nullOrVal(parts[7]);
            includeExtents = Boolean.parseBoolean(parts[8]);
            excludeEmpty = Boolean.parseBoolean(parts[9]);
            total = Integer.parseInt(parts[10]);
            pageSize = Integer.parseInt(parts[11]);
        }



        public CatalogPage(String curElement, CatalogRequestParameters params) {
            String[] parts = curElement.split("/");
            this.curOffice = parts[0];
            this.cursorId = parts[1];

            this.searchOffice = params.getOffice();
            this.idLike = params.getIdLike();
            this.locCategoryLike = params.getLocCatLike();
            this.locGroupLike = params.getLocGroupLike();
            this.tsCategoryLike = params.getTsCatLike();
            this.tsGroupLike = params.getTsGroupLike();
            this.boundingOfficeLike = params.getBoundingOfficeLike();
            this.includeExtents = params.isIncludeExtents();
            this.excludeEmpty = params.isExcludeEmpty();
        }

        private String nullOrVal(String val) {
            if (val == null || val.equalsIgnoreCase("null")) {
                return null;
            } else {
                return val;
            }
        }

        public String getSearchOffice() {
            return searchOffice;
        }

        public String getCurOffice() {
            return curOffice;
        }

        public String getCursorId() {
            return cursorId;
        }

        public int getPageSize() {
            return pageSize;
        }

        public int getTotal() {
            return total;
        }

        public String getIdLike() {
            return idLike;
        }

        public String getLocCategoryLike() {
            return locCategoryLike;
        }

        public String getLocGroupLike() {
            return locGroupLike;
        }

        public String getTsCategoryLike() {
            return tsCategoryLike;
        }

        public String getTsGroupLike() {
            return tsGroupLike;
        }

        public String getBoundingOfficeLike() {
            return boundingOfficeLike;
        }

        public boolean isIncludeExtents() {
            return includeExtents;
        }

        public boolean isExcludeEmpty() {
            return excludeEmpty;
        }


        @Override
        public String toString() {
            return curOffice + "/" + cursorId
                    + CwmsDTOPaginated.delimiter + searchOffice
                    + CwmsDTOPaginated.delimiter + idLike
                    + CwmsDTOPaginated.delimiter + locCategoryLike
                    + CwmsDTOPaginated.delimiter + locGroupLike
                    + CwmsDTOPaginated.delimiter + tsCategoryLike
                    + CwmsDTOPaginated.delimiter + tsGroupLike
                    + CwmsDTOPaginated.delimiter + boundingOfficeLike
                    + CwmsDTOPaginated.delimiter + includeExtents
                    + CwmsDTOPaginated.delimiter + excludeEmpty
                    ;
        }
    }
}
