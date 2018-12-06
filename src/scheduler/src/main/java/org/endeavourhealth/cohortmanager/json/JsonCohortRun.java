package org.endeavourhealth.cohortmanager.json;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class JsonCohortRun {

    private String organisationGroup = null;
    private String population = null;
    private String baselineDate = null;
    private String queryItemUuid = null;
    private String baselineCohortId = null;

    public JsonCohortRun() {
    }

    /**
     * gets/sets
     */
    public String getOrganisationGroup() {
        return organisationGroup;
    }

    public JsonCohortRun setOrganisationGroup(String organisationGroup) {
        this.organisationGroup = organisationGroup;
        return this;
    }

    public String getPopulation() {
        return population;
    }

    public JsonCohortRun setPopulation(String population) {
        this.population = population;
        return this;
    }

    public String getBaselineDate() {
        return baselineDate;
    }

    public JsonCohortRun setBaselineDate(String baselineDate) {
        this.baselineDate = baselineDate;
        return this;
    }

    public String getQueryItemUuid() {
        return queryItemUuid;
    }

    public JsonCohortRun setQueryItemUuid(String queryItemUuid) {
        this.queryItemUuid = queryItemUuid;
        return this;
    }

    public String getBaselineCohortId() {
        return baselineCohortId;
    }

    public JsonCohortRun setBaselineCohortId(String baselineCohortId) {
        this.baselineCohortId = baselineCohortId;
        return this;
    }


}
