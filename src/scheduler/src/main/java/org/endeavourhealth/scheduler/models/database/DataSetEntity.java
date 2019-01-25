package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "dataset", schema = "data_generator")
public class DataSetEntity {
    private int datasetId;
    private String definition;

    @Id
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSetEntity that = (DataSetEntity) o;
        return datasetId == that.datasetId &&
                Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {

        return Objects.hash(datasetId, definition);
    }

    public static DataSetEntity getDatasetDefinition(int datasetId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        DataSetEntity ret = entityManager.find(DataSetEntity.class, datasetId);

        entityManager.close();

        return ret;
    }

    public static List<DataSetEntity> getAllDatasets() throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<DataSetEntity> cq = cb.createQuery(DataSetEntity.class);
        Root<DataSetEntity> rootEntry = cq.from(DataSetEntity.class);

        /*Predicate predicate = cb.equal(rootEntry.get("isDeleted"), 0);

        cq.where(predicate);*/

        TypedQuery<DataSetEntity> query = entityManager.createQuery(cq);
        List<DataSetEntity> ret = query.getResultList();

        entityManager.close();

        return ret;
    }
}
