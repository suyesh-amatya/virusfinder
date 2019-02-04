package org.ngseq.metagenomics;

import org.apache.spark.annotation.Private;
import scala.tools.nsc.doc.model.Public;

import java.io.Serializable;

public class VirTaxa implements Serializable {


    private String gi;
    private String family;
    private String genus;
    private String species;

    public String getGi() {
        return gi;
    }

    public void setGi(String gi) {
        this.gi = gi;
    }

    public String getFamily() {return family;}

    public void setFamily(String family) {this.family= family;}

    public String getGenus() {return genus;}
    public void setGenus(String genus) {this.genus = genus;}

    public String getSpecies() {return species;}
    public void setSpecies(String species) {this.species = species;}


}
