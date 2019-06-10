package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

import java.util.ArrayList;
import java.util.List;

public class FeedbackHolder {


    private List<FileResult> fileResults = new ArrayList<>();


    public FeedbackHolder() {
    }

    public void addFileResult(FileResult fileResult) {
        fileResults.add( fileResult );
    }

    public List<FileResult> getFileResults() {
        return fileResults;
    }
}
