package org.endeavourhealth.scheduler.database;

import javax.persistence.*;

@Entity
@Table(name = "extract", schema = "data_generator")
public class ExtractEntity {
    private int extractId;
    private int cohortId;
    private int codeSetId;
    private int datasetId;

    @Id
    @Column(name = "extract_id")
    public int getExtractId() {
        return extractId;
    }

    public void setExtractId(int extractId) {
        this.extractId = extractId;
    }

    @Basic
    @Column(name = "cohort_id")
    public int getCohortId() {
        return cohortId;
    }

    public void setCohortId(int cohortId) {
        this.cohortId = cohortId;
    }

    @Basic
    @Column(name = "code_set_id")
    public int getCodeSetId() {
        return codeSetId;
    }

    public void setCodeSetId(int codeSetId) {
        this.codeSetId = codeSetId;
    }

    @Basic
    @Column(name = "dataset_id")
    public int getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(int datasetId) {
        this.datasetId = datasetId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExtractEntity that = (ExtractEntity) o;

        if (extractId != that.extractId) return false;
        if (cohortId != that.cohortId) return false;
        if (codeSetId != that.codeSetId) return false;
        if (datasetId != that.datasetId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = extractId;
        result = 31 * result + cohortId;
        result = 31 * result + codeSetId;
        result = 31 * result + datasetId;
        return result;
    }
}
