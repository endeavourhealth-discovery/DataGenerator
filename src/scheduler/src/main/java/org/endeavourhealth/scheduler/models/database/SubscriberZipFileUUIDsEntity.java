package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import java.math.BigInteger;
import java.sql.Date;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "subscriber_zip_file_uuids", schema = "data_generator")
public class SubscriberZipFileUUIDsEntity {

    private int subscriberId;
    private String batchUUID;
    private String queuedMessageUUID;
    private String queuedMessageBody;
    private long filingOrder;
    private Date fileSent;
    private Date fileFilingAttempted;
    private Boolean fileFilingSuccess;
    private String filingFailureMessage;

    @Basic
    @Column(name = "subscriber_id")
    public int getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(int subscriberId) {
        this.subscriberId = subscriberId;
    }

    @Basic
    @Column(name = "batch_uuid")
    public String getBatchUUID() {
        return batchUUID;
    }

    public void setBatchUUID(String batchUUID) {
        this.batchUUID = batchUUID;
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
    @Column(name = "queued_message_body")
    public String getQueuedMessageBody() {
        return queuedMessageBody;
    }

    public void setQueuedMessageBody(String queuedMessageBody) {
        this.queuedMessageBody = queuedMessageBody;
    }

    @Basic
    @Column(name = "filing_order")
    public long getFilingOrder() {
        return filingOrder;
    }

    public void setFilingOrder(long filingOrder) {
        this.filingOrder = filingOrder;
    }

    @Basic
    @Column(name = "file_sent")
    public Date getFileSent() {
        return fileSent;
    }

    public void setFileSent(Date fileSent) {
        this.fileSent = fileSent;
    }

    @Basic
    @Column(name = "file_filing_attempted")
    public Date getFileFilingAttempted() {
        return fileFilingAttempted;
    }

    public void setFileFilingAttempted(Date fileFilingAttempted) {
        this.fileFilingAttempted = fileFilingAttempted;
    }

    @Basic
    @Column(name = "file_filing_success")
    public Boolean getFileFilingSuccess() {
        return fileFilingSuccess;
    }

    public void setFileFilingSuccess(Boolean fileFilingSuccess) {
        this.fileFilingSuccess = fileFilingSuccess;
    }

    @Basic
    @Column(name = "filing_failure_message")
    public String getFilingFailureMessage() {
        return filingFailureMessage;
    }

    public void setFilingFailureMessage(String filingFailureMessage) {
        this.filingFailureMessage = filingFailureMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriberZipFileUUIDsEntity that = (SubscriberZipFileUUIDsEntity) o;
        return subscriberId == that.subscriberId &&
                Objects.equals(batchUUID, that.batchUUID) &&
                Objects.equals(queuedMessageUUID, that.queuedMessageUUID) &&
                Objects.equals(queuedMessageBody, that.queuedMessageBody) &&
                filingOrder == that.filingOrder &&
                Objects.equals(fileSent, that.fileSent) &&
                Objects.equals(fileFilingAttempted, that.fileFilingAttempted) &&
                Objects.equals(fileFilingSuccess, that.fileFilingSuccess) &&
                Objects.equals(filingFailureMessage, that.filingFailureMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriberId, batchUUID, queuedMessageUUID, queuedMessageBody,
                filingOrder, fileSent, fileFilingAttempted, fileFilingSuccess, filingFailureMessage);
    }

    public static SubscriberZipFileUUIDsEntity getSubscriberZipFileUUIDsEntity(String queuedMessageUUID) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        SubscriberZipFileUUIDsEntity ret = entityManager.find(SubscriberZipFileUUIDsEntity.class, queuedMessageUUID);
        entityManager.close();
        return ret;
    }

    public static List<SubscriberZipFileUUIDsEntity> getAllSubscriberZipFileUUIDsEntities() throws Exception {

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
        BigInteger bigResult = (BigInteger) query.getSingleResult();
        Long longResult = bigResult.longValue();

        if (longResult == null) {
            szfue.setFilingOrder(1);
        }
        else {
            szfue.setFilingOrder(longResult + 1);
        }

        entityManager.getTransaction().begin();
        entityManager.persist(szfue);
        entityManager.getTransaction().commit();
        entityManager.close();
        return szfue;
    }

}
