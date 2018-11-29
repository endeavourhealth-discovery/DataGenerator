package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

public class PatientExtracts {

    public static List runBulkPatientExtract(int extractId) throws Exception {
        System.out.println("bulk all patients");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT p.* FROM pcr.patient p " +
                    " left outer join pcr.patient_address pa on pa.address_id = p.home_address_id " +
                    " left outer join pcr.address a on a.id = pa.address_id " +
                    " join data_generator.cohort_results cr on cr.patient_id = p.id and cr.extract_id = :extractId " +
                    " where cr.bulked = 0;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runDeltaPatientExtract(int extractId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        System.out.println("delta all patients");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT p.* FROM pcr.patient p " +
                    " join data_generator.cohort_results cr on cr.patient_id = p.id and cr.extract_id = :extractId " +
                    " join (select item_id from pcr.event_log e " +
                    "       where e.table_id = 8 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "        group by item_id) log on log.item_id = p.id " +
                    " where cr.bulked = 1 ";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("currentTransactionId", currentTransactionId)
                    .setParameter("maxTransactionId", maxTransactionId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }
}
