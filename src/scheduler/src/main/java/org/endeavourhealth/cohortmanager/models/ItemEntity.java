package org.endeavourhealth.cohortmanager.models;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

public class ItemEntity {
    private String itemUuid;
    private String auditUuid;
    private String xmlContent;
    private String title;
    private String description;
    private byte isDeleted;

    public String getItemUuid() {
        return itemUuid;
    }

    public void setItemUuid(String itemUuid) {
        this.itemUuid = itemUuid;
    }

    public String getAuditUuid() {
        return auditUuid;
    }

    public void setAuditUuid(String auditUuid) {
        this.auditUuid = auditUuid;
    }

    public String getXmlContent() {
        return xmlContent;
    }

    public void setXmlContent(String xmlContent) {
        this.xmlContent = xmlContent;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public byte getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(byte isDeleted) {
        this.isDeleted = isDeleted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ItemEntity that = (ItemEntity) o;

        if (isDeleted != that.isDeleted) return false;
        if (itemUuid != null ? !itemUuid.equals(that.itemUuid) : that.itemUuid != null) return false;
        if (auditUuid != null ? !auditUuid.equals(that.auditUuid) : that.auditUuid != null) return false;
        if (xmlContent != null ? !xmlContent.equals(that.xmlContent) : that.xmlContent != null) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = itemUuid != null ? itemUuid.hashCode() : 0;
        result = 31 * result + (auditUuid != null ? auditUuid.hashCode() : 0);
        result = 31 * result + (xmlContent != null ? xmlContent.hashCode() : 0);
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (int) isDeleted;
        return result;
    }



}
