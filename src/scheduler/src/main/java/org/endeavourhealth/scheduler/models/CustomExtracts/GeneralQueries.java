package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.DGPersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.math.BigInteger;

public class GeneralQueries {

    public static Long getMaxTransactionId() throws Exception {
        EntityManager entityManager = DGPersistenceManager.getEntityManager();

        try {
            String sql = "select max(id) from pcr.event_log;";
            Query query = entityManager.createNativeQuery(sql);

            BigInteger result = (BigInteger)query.getSingleResult();

            return result.longValue();

        } finally {
            entityManager.close();
        }
    }

    public static void setBulkedStatus(int extractId) throws Exception {
        EntityManager entityManager = DGPersistenceManager.getEntityManager();

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
        EntityManager entityManager = DGPersistenceManager.getEntityManager();

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
}