package org.endeavourhealth.scheduler.models.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

public class ExtractSQL {

    public static List runBulkObservationAllCodesQuery(int extractId, int codeSetId) throws Exception {
        System.out.println("bulk all");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT o.* FROM data_generator.cohort_results cr" +
                    " join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    " and csc.code_set_id = :codeSetId" +
                    "  where cr.bulked = 0;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runDeltaObservationAllCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        System.out.println("delta all");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct o.* " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr.event_log e " +
                    "       where e.table_id = 32 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = o.id " +
                    " where cr.bulked = 1;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("currentTransactionId", currentTransactionId)
                    .setParameter("maxTransactionId", maxTransactionId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runBulkObservationEarliestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        System.out.println("bulk earliest each");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct o.* " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr.observation oo on oo.patient_id = o.patient_id " +
                    "   and oo.original_code = o.original_code " +
                    "   and (o.effective_date < oo.effective_date " +
                    "     or (o.effective_date = oo.effective_date and o.id < oo.id)) " +
                    " where oo.patient_id is null " +
                    "   and cr.bulked = 0;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runDeltaObservationEarliestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        System.out.println("delta earliest each");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct o.* " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr.observation oo on oo.patient_id = o.patient_id " +
                    "    and oo.original_code = o.original_code " +
                    "    and (o.effective_date < oo.effective_date " +
                    "       or (o.effective_date = oo.effective_date and o.id < oo.id)) " +
                    " join (select item_id from pcr.event_log e " +
                    " where e.table_id = 32 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = o.id " +
                    " where cr.bulked = 1 " +
                    "   and oo.patient_id is null;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("currentTransactionId", currentTransactionId)
                    .setParameter("maxTransactionId", maxTransactionId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runBulkObservationLatestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        System.out.println("bulk latest each");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct o.* " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr.observation oo on oo.patient_id = o.patient_id " +
                    "   and oo.original_code = o.original_code " +
                    "   and (o.effective_date > oo.effective_date " +
                    "     or (o.effective_date = oo.effective_date and o.id > oo.id)) " +
                    " where oo.patient_id is null " +
                    "   and cr.bulked = 0;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runDeltaObservationLatestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        System.out.println("delta latest each");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct o.* " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr.observation oo on oo.patient_id = o.patient_id " +
                    "    and oo.original_code = o.original_code " +
                    "    and (o.effective_date > oo.effective_date " +
                    "       or (o.effective_date = oo.effective_date and o.id > oo.id)) " +
                    " join (select item_id from pcr.event_log e " +
                    " where e.table_id = 32 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = o.id " +
                    " where cr.bulked = 1 " +
                    "   and oo.patient_id is null;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("currentTransactionId", currentTransactionId)
                    .setParameter("maxTransactionId", maxTransactionId);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runBulkObservationLatestCodesQuery(int extractId, int codeSetId) throws Exception {
        // build the temp table to use for subsequent query
        System.out.println("bulk latest");
        createMatchingObservationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct mc.* " +
                    " from matching_codes mc " +
                    " left join matching_codes mcoo on mcoo.patient_id = mc.patient_id " +
                    "   and (mc.effective_date > mcoo.effective_date " +
                    "     or (mc.effective_date = mcoo.effective_date and mc.id > mcoo.id)) " +
                    " where mcoo.patient_id is null;";
            Query query = entityManager.createNativeQuery(sql);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
            deleteMatchingObservationCodesTempTable();
        }
    }

    public static List runDeltaObservationLatestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // build the temp table to use for subsequent query
        System.out.println("delta latest");
        createDeltaMatchingObservationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct mc.* " +
                    " from matching_codes mc " +
                    " left join matching_codes mcoo on mcoo.patient_id = mc.patient_id " +
                    "   and (mc.effective_date > mcoo.effective_date " +
                    "     or (mc.effective_date = mcoo.effective_date and mc.id > mcoo.id)) " +
                    " where mcoo.patient_id is null;";
            Query query = entityManager.createNativeQuery(sql);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
            deleteMatchingObservationCodesTempTable();
        }
    }

    public static List runBulkObservationEarliestCodesQuery(int extractId, int codeSetId) throws Exception {
        // build the temp table to use for subsequent query
        System.out.println("bulk earliest");
        createMatchingObservationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct mc.* " +
                    " from matching_codes mc " +
                    " left join matching_codes mcoo on mcoo.patient_id = mc.patient_id " +
                    "   and (mc.effective_date < mcoo.effective_date " +
                    "     or (mc.effective_date = mcoo.effective_date and mc.id < mcoo.id)) " +
                    " where mcoo.patient_id is null;";
            Query query = entityManager.createNativeQuery(sql);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
            deleteMatchingObservationCodesTempTable();
        }
    }

    public static List runDeltaObservationEarliestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // build the temp table to use for subsequent query
        System.out.println("delta earliest");
        createDeltaMatchingObservationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct mc.* " +
                    " from matching_codes mc " +
                    " left join matching_codes mcoo on mcoo.patient_id = mc.patient_id " +
                    "   and (mc.effective_date < mcoo.effective_date " +
                    "     or (mc.effective_date = mcoo.effective_date and mc.id < mcoo.id)) " +
                    " where mcoo.patient_id is null;";
            Query query = entityManager.createNativeQuery(sql);

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
            deleteMatchingObservationCodesTempTable();
        }
    }

    public static void createMatchingObservationCodesTempTable(int extractId, int codeSetId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        System.out.println("matching codes");

        try {
            String sql = "create table matching_codes as " +
                    " select o.* " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId" +
                    " where cr.bulked = 0;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }

    public static void createDeltaMatchingObservationCodesTempTable(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        System.out.println("delta matching codes");

        try {
            String sql = "create table matching_codes as " +
                    " select o.* " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr.event_log e " +
                    "       where e.table_id = 32 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = o.id " +
                    " where cr.bulked = 1;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("currentTransactionId", currentTransactionId)
                    .setParameter("maxTransactionId", maxTransactionId);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }

    public static void deleteMatchingObservationCodesTempTable() throws Exception {
        System.out.println("delete matching codes");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "drop table matching_codes;";
            Query query = entityManager.createNativeQuery(sql);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

        } finally {
            entityManager.close();
        }
    }
}
