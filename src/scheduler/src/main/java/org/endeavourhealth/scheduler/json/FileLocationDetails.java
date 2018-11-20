package org.endeavourhealth.scheduler.json;

public class FileLocationDetails {

    private String source = null;
    private String destination = null;
    private String housekeep = null;

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

}
