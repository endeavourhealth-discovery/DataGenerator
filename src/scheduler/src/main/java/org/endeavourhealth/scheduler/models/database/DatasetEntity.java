package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name = "dataset", schema = "data_generator")
public class DatasetEntity {
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
        DatasetEntity that = (DatasetEntity) o;
        return datasetId == that.datasetId &&
                Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {

        return Objects.hash(datasetId, definition);
    }

    public static DatasetEntity getDatasetDefinition(int datasetId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        DatasetEntity ret = entityManager.find(DatasetEntity.class, datasetId);

        entityManager.close();

        return ret;
    }
}
