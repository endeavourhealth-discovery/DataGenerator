package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.math.BigInteger;
import java.util.List;

public class GeneralQueries {

    public static Long getMaxTransactionId() throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select max(id) from pcr2.event_log;";
            Query query = entityManager.createNativeQuery(sql);

            BigInteger result = (BigInteger)query.getSingleResult();

            return result.longValue();

        } finally {
            entityManager.close();
        }
    }

    public static void setBulkedStatus(int extractId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "update data_generator.cohort_results " +
                    " set bulked = 1 " +
                    " where extract_id = :extractId";

            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }

    public static void setTransactionId(int extractId, Long maxTransactionId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "update data_generator.extract " +
                    " set transaction_id = :maxTransactionId " +
                    " where extract_id = :extractId";

            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("maxTransactionId", maxTransactionId)
                    .setParameter("extractId", extractId);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }

    public static void dropMatchingObservationCodesTempTable() throws Exception {
        // System.out.println("delete matching codes");
        // LOG.info("Delete matching codes observation temp table");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "drop table if exists matching_codes;";
            Query query = entityManager.createNativeQuery(sql);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }

    public static List getDeletionsForTable(int tableId, int extractId, Long currentTransactionId, Long maxTransactionId) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select e.item_id, e.table_id from pcr2.event_log e " +
                    " join data_generator.exported_ids id on id.item_id = e.item_id and id.table_id = e.table_id " +
                    " where e.entry_mode = 1" +
                    "   and e.table_id = :tableId" +
                    "   and id.extract_id = :extractId" +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId ;";

            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("tableId", tableId)
                    .setParameter("extractId", extractId)
                    .setParameter("currentTransactionId", currentTransactionId)
                    .setParameter("maxTransactionId", maxTransactionId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static void createIndexesOnMatchingObservationCodesTempTable() throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String patientIndex = "alter table matching_codes add index codes_patient_id (patient_id);";
            String dateIndex = "alter table matching_codes add index codes_effective_date (effective_date);";
            Query query = entityManager.createNativeQuery(patientIndex);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

            query = entityManager.createNativeQuery(dateIndex);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }
}