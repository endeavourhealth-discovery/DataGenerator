package org.endeavourhealth.cohortmanager.json;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonOrganisationGroup {

    private Integer id = null;
    private String name = null;
    private List<JsonOrganisation> organisations = new ArrayList<>();

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<JsonOrganisation> getOrganisations() {
        return organisations;
    }

    public void setOrganisations(List<JsonOrganisation> organisations) {
        this.organisations = organisations;
    }
}
