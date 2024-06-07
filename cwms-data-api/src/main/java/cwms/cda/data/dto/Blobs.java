package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonRootName("blobs")
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class Blobs extends CwmsDTOPaginated {
    @JacksonXmlElementWrapper(localName = "blobs")
    @JacksonXmlProperty(localName = "blob")

    @Schema(description = "List of retrieved blobs")
    List<Blob> blobs;

    @SuppressWarnings("unused") // for JAXB to handle marshalling
    private Blobs() {
    }


    private Blobs(String cursor, int pageSize, int total) {
        super(cursor, pageSize, total);
        blobs = new ArrayList<>();
    }

    public List<Blob> getBlobs() {
        return Collections.unmodifiableList(blobs);
    }

    public static class Builder {
        private Blobs workingBlobs;

        public Builder(String cursor, int pageSize, int total) {
            workingBlobs = new Blobs(cursor, pageSize, total);
        }

        public Blobs build() {
            if (this.workingBlobs.blobs.size() == this.workingBlobs.pageSize) {
                this.workingBlobs.nextPage = encodeCursor(
                        this.workingBlobs.blobs.get(this.workingBlobs.blobs.size() - 1).toString().toUpperCase(),
                        this.workingBlobs.pageSize,
                        this.workingBlobs.total);
            } else {
                this.workingBlobs.nextPage = null;
            }
            return workingBlobs;


        }

        public Builder addBlob(Blob blob) {
            this.workingBlobs.blobs.add(blob);
            return this;
        }

        public Builder addAll(List<Blob> toAdd) {
            this.workingBlobs.blobs.addAll(toAdd);
            return this;
        }

    }

    @Override
    public void validate() throws FieldException {
        // always valid even if just empty list.
    }
}
