package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

public class FailureResult extends Result {

    String message;

    @Override
    public String toString() {
        return "FailureResult{" +
                "message='" + message + '\'' +
                ", uuid='" + uuid + '\'' +
                '}';
    }

    public FailureResult(String s) {
        super();

        String[] fields = s.split(",");

        setUuid( fields[0] );
        setMessage( fields[1] );
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
