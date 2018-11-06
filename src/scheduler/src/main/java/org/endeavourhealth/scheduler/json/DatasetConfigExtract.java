package org.endeavourhealth.scheduler.json;

import java.util.List;

public class DatasetConfigExtract {
    private String type = null;
    private String fields = null;
    private List<DatasetParameter> parameters;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }

    public List<DatasetParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<DatasetParameter> parameters) {
        this.parameters = parameters;
    }
}
