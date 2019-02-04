package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "extract", schema = "data_generator")
public class ExtractEntity {
    private int extractId;
    private String extractName;
    private int cohortId;
    private int codeSetId;
    private int datasetId;
    private String definition;
    private long transactionId;
    private String cron;
    private Byte clearCohortEveryRun;

    @Id
    @Column(name = "extract_id")
    public int getExtractId() {
        return extractId;
    }

    public void setExtractId(int extractId) {
        this.extractId = extractId;
    }

    @Basic
    @Column(name = "extract_name")
    public String getExtractName() {
        return extractName;
    }

    public void setExtractName(String extractName) {
        this.extractName = extractName;
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

    @Basic
    @Column(name = "definition")
    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    @Basic
    @Column(name = "transaction_id")
    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    @Basic
    @Column(name = "cron")
    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtractEntity that = (ExtractEntity) o;
        return extractId == that.extractId &&
                cohortId == that.cohortId &&
                codeSetId == that.codeSetId &&
                datasetId == that.datasetId &&
                transactionId == that.transactionId &&
                Objects.equals(extractName, that.extractName) &&
                Objects.equals(definition, that.definition) &&
                Objects.equals(cron, that.cron);
    }

    @Override
    public int hashCode() {

        return Objects.hash(extractId, extractName, cohortId, codeSetId, datasetId, definition, transactionId, cron);
    }

    @Basic
    @Column(name = "clear_cohort_every_run")
    public Byte getClearCohortEveryRun() {
        return clearCohortEveryRun;
    }

    public void setClearCohortEveryRun(Byte clearCohortEveryRun) {
        this.clearCohortEveryRun = clearCohortEveryRun;
    }

    public static List<ExtractEntity> getAllExtracts() throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExtractEntity> cq = cb.createQuery(ExtractEntity.class);
        Root<ExtractEntity> rootEntry = cq.from(ExtractEntity.class);

        /*Predicate predicate = cb.equal(rootEntry.get("isDeleted"), 0);

        cq.where(predicate);*/

        TypedQuery<ExtractEntity> query = entityManager.createQuery(cq);
        List<ExtractEntity> ret = query.getResultList();

        entityManager.close();

        return ret;
    }

    public static ExtractEntity getExtract(int extractId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        ExtractEntity ret = entityManager.find(ExtractEntity.class, extractId);

        entityManager.close();

        return ret;
    }

    public static ExtractEntity deleteExtract(int extractId) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        entityManager.getTransaction().begin();
        ExtractEntity entry = entityManager.find(ExtractEntity.class, extractId);
        entry = entityManager.merge(entry);
        entityManager.remove(entry);
        entityManager.getTransaction().commit();
        entityManager.close();

        return entry;
    }

    public static ExtractEntity createExtract(ExtractEntity extract) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();

        String sql = "select max(extract_id) from data_generator.extract;";
        Query query = entityManager.createNativeQuery(sql);
        Integer result = (Integer) query.getSingleResult();
        extract.setExtractId(result + 1);

        entityManager.getTransaction().begin();
        entityManager.persist(extract);
        entityManager.getTransaction().commit();
        entityManager.close();

        return extract;
    }

    public static ExtractEntity updateExtract(ExtractEntity extract) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        entityManager.getTransaction().begin();
        entityManager.merge(extract);
        entityManager.getTransaction().commit();
        entityManager.close();

        return extract;
    }
}