package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

public class Result {

    String uuid;
    String errorMessage;

    public Result(String uuid) {
        this();
        this.uuid = uuid;
    }

    public Result() {
        super();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void addError(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
