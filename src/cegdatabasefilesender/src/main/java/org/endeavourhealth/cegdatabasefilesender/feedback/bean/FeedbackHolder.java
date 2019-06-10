package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

import java.util.List;

public class FeedbackHolder {

    private final List<FailureResult> failureResults;

    private final List<SuccessResult> successResults;

    private final List<Result> errors;


    public FeedbackHolder(List<FailureResult> failureResults, List<SuccessResult> successResults,  List<Result> errors) {
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

    public List<Result> getErrors() {
        return errors;
    }

    public void addError(Result result) {

        errors.add( result );
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
