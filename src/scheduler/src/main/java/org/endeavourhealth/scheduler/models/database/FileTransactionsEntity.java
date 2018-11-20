package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "file_transactions", schema = "data_generator")
public class FileTransactionsEntity {

    private long transaction_id;
    private String filename;
    private Timestamp extract_date;
    private Timestamp zip_date;
    private Timestamp encrypt_date;
    private Timestamp sftp_date;
    private Timestamp housekeeping_date;

    @Id
    @Column(name = "transaction_id")
    public long getTransactionId() {
        return transaction_id;
    }

    public void setTransactionId(long transaction_id) {
        this.transaction_id = transaction_id;
    }

    @Basic
    @Column(name = "filename")
    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    @Basic
    @Column(name = "extract_date")
    public Timestamp getExtractDate() {
        return extract_date;
    }

    public void setExtractDate(Timestamp extract_date) {
        this.extract_date = extract_date;
    }

    @Basic
    @Column(name = "zip_date")
    public Timestamp getZipDate() {
        return zip_date;
    }

    public void setZipDate(Timestamp zip_date) {
        this.zip_date = zip_date;
    }

    @Basic
    @Column(name = "encrypt_date")
    public Timestamp getEncryptDate() {
        return encrypt_date;
    }

    public void setEncryptDate(Timestamp encrypt_date) {
        this.encrypt_date = encrypt_date;
    }

    @Basic
    @Column(name = "sftp_date")
    public Timestamp getSftpDate() {
        return sftp_date;
    }

    public void setSftpDate(Timestamp sftp_date) {
        this.sftp_date = sftp_date;
    }

    @Basic
    @Column(name = "housekeeping_date")
    public Timestamp getHousekeepingDate() {
        return housekeeping_date;
    }

    public void setHousekeepingDate(Timestamp housekeeping_date) {
        this.housekeeping_date = housekeeping_date;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FileTransactionsEntity that = (FileTransactionsEntity) obj;
        return transaction_id == that.transaction_id &&
                filename.equals(that.filename) &&
                extract_date.equals(that.extract_date) &&
                zip_date.equals(that.zip_date) &&
                encrypt_date.equals(that.encrypt_date) &&
                sftp_date.equals(that.sftp_date) &&
                housekeeping_date.equals(that.housekeeping_date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction_id,
                filename,
                extract_date,
                zip_date,
                encrypt_date,
                sftp_date,
                housekeeping_date);
    }

    public static void create(FileTransactionsEntity entry) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        entityManager.getTransaction().begin();
        entityManager.persist(entry);
        entityManager.getTransaction().commit();
    }

    public static void update(FileTransactionsEntity entry) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        entityManager.getTransaction().begin();
        entityManager.merge(entry);
        entityManager.getTransaction().commit();
    }

    public static void delete(FileTransactionsEntity entry) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        entityManager.getTransaction().begin();
        entityManager.remove(entry);
        entityManager.getTransaction().commit();
    }

    public static List<FileTransactionsEntity> getFilesForResending(long transaction_id) throws Exception {
        return getFileTransactionsValues(transaction_id, false, false,
                false, false, false);
    }

    public static List<FileTransactionsEntity> getFilesForZip() throws Exception {
        return getFileTransactionsValues(null, false, true,
                true, true, true);
    }

    public static List<FileTransactionsEntity> getFilesForEncryption() throws Exception {
        return getFileTransactionsValues(null,false, false,
                true, true, true);
    }

    public static List<FileTransactionsEntity> getFilesForSftp() throws Exception {
        return getFileTransactionsValues(null, false,false,
                false, true, true);
    }

    public static List<FileTransactionsEntity> getFilesForHousekeeping() throws Exception {
        return getFileTransactionsValues(null, false,false,
                false,false, true);
    }

    private static List<FileTransactionsEntity> getFileTransactionsValues(Long transaction_id,
        boolean extractIsNull, boolean zipIsNull, boolean encryptIsNull,
        boolean sftpIsNull, boolean isHousekeepingIsNull) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<FileTransactionsEntity> query = builder.createQuery(FileTransactionsEntity.class);
        Root<FileTransactionsEntity> root = query.from(FileTransactionsEntity.class);
        List<Predicate> predicates = new ArrayList();

        if (transaction_id != null) {
            predicates.add(builder.equal(root.get("transactionId"), transaction_id));
        }

        if (extractIsNull) {
            predicates.add(builder.isNull(root.get("extractDate")));
        } else {
            predicates.add(builder.isNotNull(root.get("extractDate")));
        }

        if (encryptIsNull) {
            predicates.add(builder.isNull(root.get("encryptDate")));
        } else {
            predicates.add(builder.isNotNull(root.get("encryptDate")));
        }

        if (zipIsNull) {
            predicates.add(builder.isNull(root.get("zipDate")));
        } else {
            predicates.add(builder.isNotNull(root.get("zipDate")));
        }

        if (sftpIsNull) {
            predicates.add(builder.isNull(root.get("sftpDate")));
        } else {
            predicates.add(builder.isNotNull(root.get("sftpDate")));
        }

        if (isHousekeepingIsNull) {
            predicates.add(builder.isNull(root.get("housekeepingDate")));
        } else {
            predicates.add(builder.isNotNull(root.get("housekeepingDate")));
        }

        query.where(predicates.toArray(new Predicate[0]));
        TypedQuery<FileTransactionsEntity> tq = entityManager.createQuery(query);
        List<FileTransactionsEntity> ret = tq.getResultList();

        entityManager.close();

        return ret;
    }
}
