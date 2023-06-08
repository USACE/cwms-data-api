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

@XmlRootElement(name="clobs")
@XmlSeeAlso(Clob.class)
@XmlAccessorType(XmlAccessType.FIELD)
public class Clobs extends CwmsDTOPaginated {
    @XmlElementWrapper
    @XmlElement(name="clob")
    // Use the array shape to optimize data transfer to client
    //@JsonFormat(shape=JsonFormat.Shape.ARRAY)
    @Schema(description = "List of retrieved clobs")
    List<Clob> clobs;



    @SuppressWarnings("unused") // for JAXB to handle marshalling
    private Clobs(){}


    private Clobs(String cursor, int pageSize, int total){
        super(cursor, pageSize, total);
        clobs = new ArrayList<>();
    }

    public List<Clob> getClobs() {
        return Collections.unmodifiableList(clobs);
    }

    private void addClob(Clob clob ){
        clobs.add(clob);
    }

    public static class Builder {
        private Clobs workingClobs = null;
        public Builder( String cursor, int pageSize, int total){
            workingClobs = new Clobs(cursor, pageSize, total);
        }

        public Clobs build(){
            if( this.workingClobs.clobs.size() == this.workingClobs.pageSize){
                this.workingClobs.nextPage = encodeCursor(
                            this.workingClobs.clobs.get(this.workingClobs.clobs.size()-1).toString().toUpperCase(),
                            this.workingClobs.pageSize,
                            this.workingClobs.total);
            } else {
                this.workingClobs.nextPage = null;
            }
            return workingClobs;


        }

        public Builder addClob(Clob clob){
            this.workingClobs.addClob(clob);
            return this;
        }
    }

    @Override
    public void validate() throws FieldException {
         // Clobs always contains a valid array even empty.
    }

}
