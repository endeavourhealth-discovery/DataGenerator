package org.endeavourhealth.scheduler.json.DatasetDefinition;

import java.util.List;

public class DatasetConfigExtract {
    private String type = null;
    private List<DatasetFields> fields;
    private List<DatasetCodeSet> codeSets;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<DatasetFields> getFields() {
        return fields;
    }

    public void setFields(List<DatasetFields> fields) {
        this.fields = fields;
    }

    public List<DatasetCodeSet> getCodeSets() {
        return codeSets;
    }

    public void setCodeSets(List<DatasetCodeSet> codeSets) {
        this.codeSets = codeSets;
    }
}
