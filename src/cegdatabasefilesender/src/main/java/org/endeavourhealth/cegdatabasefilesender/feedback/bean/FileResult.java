package org.endeavourhealth.cegdatabasefilesender.feedback.bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileResult {

    String filepath;

    List<Result> results = new ArrayList<>();

    public FileResult(String filepath) {
        this.filepath = filepath;
    }

    public String getFilepath() {
        return filepath;
    }

    public List<Result> getErrors() {
        return results.stream().filter( r -> r.getType() == ResultType.ERROR).collect(Collectors.toList());
    }


    public void addError(String errorMessage) {
        results.add( new Result(ResultType.ERROR, null, errorMessage) );
    }


    public void addFailure(String failure) {
        String[] fields = failure.split(",");
        results.add( new Result(ResultType.FAILURE, fields[0], fields[1]) );
    }

    public boolean hasErrors() {
        return results.stream().anyMatch( r -> r.getType() == ResultType.ERROR);
    }

    public void addSuccess(String success) {

        List<Result> successList = Arrays.stream( success.split(System.getProperty("line.separator")) )
                .map( s -> new Result( ResultType.SUCCESS, s, null) )
                .collect(Collectors.toList());

        results.addAll(successList);
    }

    public List<Result> getResults() {
        return results;
    }
}
