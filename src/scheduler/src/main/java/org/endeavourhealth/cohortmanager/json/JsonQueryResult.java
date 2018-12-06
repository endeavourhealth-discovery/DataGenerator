package org.endeavourhealth.cohortmanager.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.cohortmanager.PercentageCalculator;
import org.endeavourhealth.cohortmanager.models.ItemEntity;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class JsonQueryResult {

    private String name = null;
    private String description = null;
    private String uuid = null;
    private Integer resultCount = null;
    private String percentageOfParent = null;
    private List<JsonQueryResult> childQueries = null;

    public JsonQueryResult() {}

    public JsonQueryResult(ItemEntity item, Integer resultCount, Integer parentCount) {
        this.name = item.getTitle();
        this.description = item.getDescription();
        this.uuid = item.getItemUuid();
        this.resultCount = resultCount;

        if (resultCount != null && parentCount != null) {
            this.percentageOfParent = PercentageCalculator.calculatorPercentString(parentCount.doubleValue(), resultCount.intValue());
        }
    }

    public void addChildReult(JsonQueryResult result) {
        if (childQueries == null) {
            childQueries = new ArrayList<>();
        }
        childQueries.add(result);
    }


    /**
     * gets/sets
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getPercentageOfParent() {
        return percentageOfParent;
    }

    public void setPercentageOfParent(String percentageOfParent) {
        this.percentageOfParent = percentageOfParent;
    }

    public Integer getResultCount() {
        return resultCount;
    }

    public void setResultCount(Integer resultCount) {
        this.resultCount = resultCount;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public List<JsonQueryResult> getChildQueries() {
        return childQueries;
    }

    public void setChildQueries(List<JsonQueryResult> childQueries) {
        this.childQueries = childQueries;
    }
}
