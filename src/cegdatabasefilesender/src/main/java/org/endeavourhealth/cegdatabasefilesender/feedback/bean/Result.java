package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

public class Result {

    String uuid;

    String errorMessage;

    ResultType type;

    public Result(String uuid) {
        this();
        this.uuid = uuid;
    }

    public Result(String uuid, String failureMessage) {
        this(uuid);
        this.errorMessage = failureMessage;
    }

    public Result(ResultType type, String uuid, String failureMessage) {
        this(uuid, failureMessage);
        setType(type);
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ResultType getType() {
        return type;
    }

    public void setType(ResultType type) {
        this.type = type;
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


