package org.endeavourhealth.scheduler.json;

import java.util.List;

public class DatasetConfig {
    private String name = null;
    private Integer id = null;
    private List<DatasetConfigExtract> extract;

    private DatasetConfig() {}

    private DatasetConfig(String name, Integer id, List<DatasetConfigExtract> extract) {
        this.name = name;
        this.id = id;
        this.extract = extract;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public List<DatasetConfigExtract> getExtract() {
        return extract;
    }

    public void setExtract(List<DatasetConfigExtract> extract) {
        this.extract = extract;
    }
}
