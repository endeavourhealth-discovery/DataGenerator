package org.endeavourhealth.scheduler.json.ExtractDefinition;

public class FileLocationDetails {

    private String source = null;
    private String destination = null;
    private String housekeep = null;
    private String certificate = null;

    /* private FileLocationDetails() {}

    private FileLocationDetails(String source, String destination) {
        this.source = source;
        this.destination = destination;
    } */

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getHousekeep() {
        return housekeep;
    }

    public void setHousekeep(String housekeep) {
        this.housekeep = housekeep;
    }

    public String getCertificate() { return certificate; }

    public void setCertificate(String certificate) { this.certificate = certificate; }
}
