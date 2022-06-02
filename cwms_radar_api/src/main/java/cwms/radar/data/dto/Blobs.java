package cwms.radar.data.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import cwms.radar.api.errors.FieldException;
import io.swagger.v3.oas.annotations.media.Schema;

@XmlRootElement(name="blobs")
@XmlSeeAlso(Blob.class)
@XmlAccessorType(XmlAccessType.FIELD)
public class Blobs extends CwmsDTOPaginated {
    @XmlElementWrapper
    @XmlElement(name="blob")
    // Use the array shape to optimize data transfer to client
    //@JsonFormat(shape=JsonFormat.Shape.ARRAY)
    @Schema(implementation = Blob.class, description = "List of retrieved blobs")
    List<Blob> blobs;



    @SuppressWarnings("unused") // for JAXB to handle marshalling
    private Blobs(){}


    private Blobs(String cursor, int pageSize, int total){
        super(cursor, pageSize, total);
        blobs = new ArrayList<>();
    }

    public List<Blob> getBlobs() {
        return Collections.unmodifiableList(blobs);
    }

    public static class Builder {
        private Blobs workingBlobs = null;
        public Builder( String cursor, int pageSize, int total){
            workingBlobs = new Blobs(cursor, pageSize, total);
        }

        public Blobs build(){
            if( this.workingBlobs.blobs.size() == this.workingBlobs.pageSize){
                this.workingBlobs.nextPage = encodeCursor(
                            this.workingBlobs.blobs.get(this.workingBlobs.blobs.size()-1).toString().toUpperCase(),
                            this.workingBlobs.pageSize,
                            this.workingBlobs.total);
            } else {
                this.workingBlobs.nextPage = null;
            }
            return workingBlobs;


        }

        public Builder addBlob(Blob blob){
            this.workingBlobs.blobs.add(blob);
            return this;
        }

        public Builder addAll(List<Blob> toAdd ){
            this.workingBlobs.blobs.addAll(toAdd);
            return this;
        }

    }

    @Override
    public void validate() throws FieldException {
        // always valid even if just empty list.
    }
}
