package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import java.util.Objects;

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
        return extractId == that.extractId &&
                patientId == that.patientId &&
                organisationId == that.organisationId;
    }

    @Override
    public int hashCode() {

        return Objects.hash(extractId, patientId, organisationId);
    }

    public static void saveCohortPatients(CohortResultsEntity result) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        entityManager.getTransaction().begin();

        entityManager.persist(result);

        entityManager.getTransaction().commit();

        entityManager.close();
    }

    public static void clearCohortResults(Integer extractId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "delete from cohort_results " +
                    " where extract_id = :extractId";

            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }


}
