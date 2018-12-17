package org.endeavourhealth.scheduler.models.database;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Objects;

public class ExportedIdsEntityPK implements Serializable {
    private int extractId;
    private long itemId;
    private int tableId;

    @Column(name = "extract_id")
    @Id
    public int getExtractId() {
        return extractId;
    }

    public void setExtractId(int extractId) {
        this.extractId = extractId;
    }

    @Column(name = "item_id")
    @Id
    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    @Column(name = "table_id")
    @Id
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
        ExportedIdsEntityPK that = (ExportedIdsEntityPK) o;
        return extractId == that.extractId &&
                itemId == that.itemId &&
                tableId == that.tableId;
    }

    @Override
    public int hashCode() {

        return Objects.hash(extractId, itemId, tableId);
    }
}
