package org.endeavourhealth.scheduler.database;

import javax.persistence.*;

@Entity
@Table(name = "cohort_results", schema = "data_generator")
public class CohortResultsEntity {
    private int extractId;
    private long patientId;
    private long organisationId;

    @Basic
    @Column(name = "extract_id")
    public int getExtractId() {
        return extractId;
    }

    public void setExtractId(int extractId) {
        this.extractId = extractId;
    }

    @Id
    @Column(name = "patient_id")
    public long getPatientId() {
        return patientId;
    }

    public void setPatientId(long patientId) {
        this.patientId = patientId;
    }

    @Basic
    @Column(name = "organisation_id")
    public long getOrganisationId() {
        return organisationId;
    }

    public void setOrganisationId(long organisationId) {
        this.organisationId = organisationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CohortResultsEntity that = (CohortResultsEntity) o;

        if (extractId != that.extractId) return false;
        if (patientId != that.patientId) return false;
        if (organisationId != that.organisationId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = extractId;
        result = 31 * result + (int) (patientId ^ (patientId >>> 32));
        result = 31 * result + (int) (organisationId ^ (organisationId >>> 32));
        return result;
    }
}
