package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

import java.util.List;

public class FeedbackHolder {

    private List<FailureResult> failureResults;

    private List<SuccessResult> successResults;


    private List<String> errors;


    public FeedbackHolder() {
        super();
    }

    public FeedbackHolder(List<FailureResult> failureResults, List<SuccessResult> successResults, List<String> errors) {
        this();
        this.failureResults = failureResults;
        this.successResults = successResults;
        this.errors = errors;
    }

    public List<FailureResult> getFailureResults() {
        return failureResults;
    }

    public List<SuccessResult> getSuccessResults() {
        return successResults;
    }

    public List<String> getErrors() {
        return errors;
    }
}
