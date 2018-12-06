package org.endeavourhealth.cohortmanager.models;

import org.endeavourhealth.cohortmanager.querydocument.models.RuleAction;

import java.util.ArrayList;
import java.util.List;

public class QueryResult {
    private Integer organisationId;
    private Integer ruleId;
    private List<Integer> patients = new ArrayList<>();
    private List<Object[]> observations = new ArrayList<>();
    private RuleAction onPass;
    private RuleAction onFail;

    public long getOrganisationId() {
        return organisationId;
    }

    public void setOrganisationId(Integer organisationId) {
        this.organisationId = organisationId;
    }

    public Integer getRuleId() {
        return ruleId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }

    public RuleAction getOnPass() {
        return onPass;
    }

    public void setOnPass(RuleAction value) {
        this.onPass = value;
    }

    public RuleAction getOnFail() {
        return onFail;
    }

    public void setOnFail(RuleAction value) {
        this.onFail = value;
    }

    public List<Integer> getPatients() { return patients; }

    public void setPatients(List<Integer> patients) { this.patients = patients; }

    public List<Object[]> getObservations() { return observations; }

    public void setObservations(List<Object[]> observations) { this.observations = observations; }

}
