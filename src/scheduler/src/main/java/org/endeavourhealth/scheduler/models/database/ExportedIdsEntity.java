package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Entity
@Table(name = "exported_ids", schema = "data_generator")
@IdClass(ExportedIdsEntityPK.class)
public class ExportedIdsEntity {
    private int extractId;
    private long itemId;
    private int tableId;

    public ExportedIdsEntity() {
    }

    public ExportedIdsEntity(int extractId, long itemId, int tableId) {
        this.extractId = extractId;
        this.itemId = itemId;
        this.tableId = tableId;
    }

    @Id
    @Column(name = "extract_id")
    public int getExtractId() {
        return extractId;
    }

    public void setExtractId(int extractId) {
        this.extractId = extractId;
    }

    @Id
    @Column(name = "item_id")
    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    @Id
    @Column(name = "table_id")
    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExportedIdsEntity that = (ExportedIdsEntity) o;
        return extractId == that.extractId &&
                itemId == that.itemId &&
                tableId == that.tableId;
    }

    @Override
    public int hashCode() {

        return Objects.hash(extractId, itemId, tableId);
    }

    public static void saveExportedIds(int extractId, int tableId, List<Long> itemIds) throws Exception {

        List<ExportedIdsEntity> exportedIdsEntities = itemIds.stream()
                .map(x -> new ExportedIdsEntity(extractId, x.longValue(), tableId))
                .collect(Collectors.toList());

        bulkSaveMappings(exportedIdsEntities);
    }

    public static void bulkSaveMappings(List<ExportedIdsEntity> exportedIdsEntities) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        int batchSize = 1000;
        entityManager.getTransaction().begin();

        for(int i = 0; i < exportedIdsEntities.size(); ++i) {
            ExportedIdsEntity entity = exportedIdsEntities.get(i);
            entityManager.merge(entity);
            if(i % batchSize == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        }

        entityManager.getTransaction().commit();
        entityManager.close();
    }
}
