package org.endeavourhealth.cohortmanager.json;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class JsonOrganisation {

    private String id = null;
    private String name = null;
    private String odsCode = null;

    public JsonOrganisation() {
    }

    /**
     * gets/sets
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOdsCode() {
        return odsCode;
    }

    public void setOdsCode(String odsCode) {
        this.odsCode = odsCode;
    }


}
