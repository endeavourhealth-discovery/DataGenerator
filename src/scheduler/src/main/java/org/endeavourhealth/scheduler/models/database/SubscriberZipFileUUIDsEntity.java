package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "subscriber_zip_file_uuids", schema = "data_generator")
public class SubscriberZipFileUUIDsEntity {

    //TODO Put code here to add in the other fields now in this table

    private int subscriberId;
    private String queuedMessageUUID;
    private int filingOrder;
    private Boolean fileSent;

    @Basic
    @Column(name = "subscriber_id")
    public int getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(int subscriberId) {
        this.subscriberId = subscriberId;
    }

    @Id
    @Column(name = "queued_message_uuid")
    public String getQueuedMessageUUID() {
        return queuedMessageUUID;
    }

    public void setQueuedMessageUUID(String queuedMessageUUID) {
        this.queuedMessageUUID = queuedMessageUUID;
    }

    @Basic
    @Column(name = "filing_order")
    public int getFilingOrder() {
        return filingOrder;
    }

    public void setFilingOrder(int filingOrder) {
        this.filingOrder = filingOrder;
    }

    @Basic
    @Column(name = "file_sent")
    public Boolean getFileSent() {
        return fileSent;
    }

    public void setFileSent(Boolean fileSent) {
        this.fileSent = fileSent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriberZipFileUUIDsEntity that = (SubscriberZipFileUUIDsEntity) o;
        return subscriberId == that.subscriberId &&
                Objects.equals(queuedMessageUUID, that.queuedMessageUUID) &&
                filingOrder == that.filingOrder &&
                Objects.equals(fileSent, that.fileSent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriberId, queuedMessageUUID, filingOrder, fileSent);
    }

    public static SubscriberZipFileUUIDsEntity getSubscriberZipFileUUIDsEntity(String queuedMessageUUID) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        SubscriberZipFileUUIDsEntity ret = entityManager.find(SubscriberZipFileUUIDsEntity.class, queuedMessageUUID);
        entityManager.close();
        return ret;
    }

    public static List<SubscriberZipFileUUIDsEntity> getAllSubcriberZipFileUUIDsEntities() throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<SubscriberZipFileUUIDsEntity> cq = cb.createQuery(SubscriberZipFileUUIDsEntity.class);
        TypedQuery<SubscriberZipFileUUIDsEntity> query = entityManager.createQuery(cq);
        List<SubscriberZipFileUUIDsEntity> ret = query.getResultList();
        entityManager.close();
        return ret;
    }

    public static SubscriberZipFileUUIDsEntity createSubscriberZipFileUUIDsEntity(
            SubscriberZipFileUUIDsEntity szfue) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();

        String sql = "select max(filing_order) from data_generator.subscriber_zip_file_uuids;";
        Query query = entityManager.createNativeQuery(sql);
        Integer result = (Integer) query.getSingleResult();
        szfue.setFilingOrder(result + 1);

        entityManager.getTransaction().begin();
        entityManager.persist(szfue);
        entityManager.getTransaction().commit();
        entityManager.close();
        return szfue;
    }

}
