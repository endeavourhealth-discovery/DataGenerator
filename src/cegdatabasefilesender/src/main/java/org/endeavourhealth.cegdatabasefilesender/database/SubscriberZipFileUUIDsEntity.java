package org.endeavourhealth.cegdatabasefilesender.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;

import javax.persistence.*;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "subscriber_zip_file_uuids", schema = "data_generator")
public class SubscriberZipFileUUIDsEntity {

    private String queuedMessageUUID;

    @Id
    @Column(name = "queued_message_uuid")
    public String getQueuedMessageUUID() {
        return queuedMessageUUID;
    }

    public void getQueuedMessageUUID(String queuedMessageUUID) {
        this.queuedMessageUUID = queuedMessageUUID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriberZipFileUUIDsEntity that = (SubscriberZipFileUUIDsEntity) o;
        return Objects.equals(queuedMessageUUID, that.queuedMessageUUID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queuedMessageUUID);
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
            SubscriberZipFileUUIDsEntity subscriberZipFileUUIDsEntity) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        entityManager.getTransaction().begin();
        entityManager.persist(subscriberZipFileUUIDsEntity);
        entityManager.getTransaction().commit();
        entityManager.close();
        return subscriberZipFileUUIDsEntity;
    }

}
