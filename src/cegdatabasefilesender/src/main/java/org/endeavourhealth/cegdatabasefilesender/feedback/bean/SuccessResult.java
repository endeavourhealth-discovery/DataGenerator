package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

public class SuccessResult extends Result {

    @Override
    public String toString() {
        return "SuccessResult{" +
                "uuid='" + uuid + '\'' +
                '}';
    }

    public SuccessResult(String uuid) {
        super(uuid);
    }
}
