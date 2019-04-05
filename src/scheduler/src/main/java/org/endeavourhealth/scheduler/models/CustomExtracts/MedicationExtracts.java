package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MedicationExtracts {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationExtracts.class);

    public static List runBulkMedicationAllCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk all");
        // LOG.info("Bulk medication all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT DISTINCT " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " FROM data_generator.cohort_results cr" +
                    " join ceg_compass_data.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    " and csc.code_set_id = :codeSetId" +
                    " where cr.bulked = 0 " +
                    " limit :index, " + size + "; ";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("index", ((page - 1) * size));

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runDeltaMedicationAllCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta all");
        // LOG.info("Delta medication all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " FROM data_generator.cohort_results cr " +
                    " join ceg_compass_data.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
                    "       where e.table_id = 44 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = m.id " +
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

    public static List runBulkMedicationEarliestEachCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk earliest each");
        // LOG.info("Bulk medication earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " from data_generator.cohort_results cr " +
                    " inner join ceg_compass_data.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join ceg_compass_data.medication_statement oo on oo.patient_id = m.patient_id " +
                    "   and oo.original_code = m.original_code " +
                    "   and (m.effective_date < oo.effective_date " +
                    "     or (m.effective_date = oo.effective_date and m.id < oo.id)) " +
                    " where oo.patient_id is null " +
                    "   and cr.bulked = 0 " +
                    " limit :index, " + size + "; ";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("index", ((page - 1) * size));

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runDeltaMedicationEarliestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest each");
        // LOG.info("Delta medication earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " FROM data_generator.cohort_results cr " +
                    " join ceg_compass_data.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join ceg_compass_data.medication_statement oo on oo.patient_id = m.patient_id " +
                    "    and oo.original_code = m.original_code " +
                    "    and (m.effective_date < oo.effective_date " +
                    "       or (m.effective_date = oo.effective_date and m.id < oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
                    " where e.table_id = 44 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = m.id " +
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

    public static List runBulkMedicationLatestEachCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk latest each");
        // LOG.info("Bulk medication latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " from data_generator.cohort_results cr " +
                    " inner join ceg_compass_data.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join ceg_compass_data.medication_statement oo on oo.patient_id = m.patient_id " +
                    "   and oo.original_code = m.original_code " +
                    "   and (m.effective_date > oo.effective_date " +
                    "     or (m.effective_date = oo.effective_date and m.id > oo.id)) " +
                    " where oo.patient_id is null " +
                    "   and cr.bulked = 0 " +
                    " limit :index, " + size + "; ";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("index", ((page - 1) * size));

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
        }
    }

    public static List runDeltaMedicationLatestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest each");
        // LOG.info("Delta medication latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " FROM data_generator.cohort_results cr " +
                    " join ceg_compass_data.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join ceg_compass_data.medication_statement oo on oo.patient_id = m.patient_id " +
                    "    and oo.original_code = m.original_code " +
                    "    and (m.effective_date > oo.effective_date " +
                    "       or (m.effective_date = oo.effective_date and m.id > oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
                    " where e.table_id = 44 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = m.id " +
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

    public static List runBulkMedicationLatestCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk latest");
        // LOG.info("Bulk medication latest of all codes");

        // build the temp table to use for subsequent query
        createMatchingMedicationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.dmd_id, " +
                    "  mc.clinical_effective_date," +
                    "  mc.date_precision_id, " +
                    "  mc.practitioner_id, " +
                    "  mc.organization_id, " +
                    "  mc.original_term, " +
                    "  date_format(mc.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  mc.dose, " +
                    "  mc.quantity_value, " +
                    "  mc.quantity_unit" +
                    " from matching_codes mc " +
                    " left join matching_codes mcoo on mcoo.patient_id = mc.patient_id " +
                    "   and (mc.effective_date > mcoo.effective_date " +
                    "     or (mc.effective_date = mcoo.effective_date and mc.id > mcoo.id)) " +
                    " where mcoo.patient_id is null " +
                    " limit :index, " + size + "; ";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("index", ((page - 1) * size));

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
            GeneralQueries.dropMatchingObservationCodesTempTable();
        }
    }

    public static List runDeltaMedicationLatestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest");
        // LOG.info("Delta medication latest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingMedicationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.dmd_id, " +
                    "  mc.clinical_effective_date," +
                    "  mc.date_precision_id, " +
                    "  mc.practitioner_id, " +
                    "  mc.organization_id, " +
                    "  mc.original_term, " +
                    "  date_format(mc.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  mc.dose, " +
                    "  mc.quantity_value, " +
                    "  mc.quantity_unit" +
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
            GeneralQueries.dropMatchingObservationCodesTempTable();
        }
    }

    public static List runBulkMedicationEarliestCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk earliest");
        // LOG.info("Bulk medication earliest of all codes");

        // build the temp table to use for subsequent query
        createMatchingMedicationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.dmd_id, " +
                    "  mc.clinical_effective_date," +
                    "  mc.date_precision_id, " +
                    "  mc.practitioner_id, " +
                    "  mc.organization_id, " +
                    "  mc.original_term, " +
                    "  date_format(mc.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  mc.dose, " +
                    "  mc.quantity_value, " +
                    "  mc.quantity_unit" +
                    " left join matching_codes mcoo on mcoo.patient_id = mc.patient_id " +
                    "   and (mc.effective_date < mcoo.effective_date " +
                    "     or (mc.effective_date = mcoo.effective_date and mc.id < mcoo.id)) " +
                    " where mcoo.patient_id is null " +
                    " limit :index, " + size + "; ";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("index", ((page - 1) * size));

            List result = query.getResultList();

            return result;

        } finally {
            entityManager.close();
            GeneralQueries.dropMatchingObservationCodesTempTable();
        }
    }

    public static List runDeltaMedicationEarliestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest");
        // LOG.info("Delta medication earliest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingMedicationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.dmd_id, " +
                    "  mc.clinical_effective_date," +
                    "  mc.date_precision_id, " +
                    "  mc.practitioner_id, " +
                    "  mc.organization_id, " +
                    "  mc.original_term, " +
                    "  date_format(mc.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  mc.dose, " +
                    "  mc.quantity_value, " +
                    "  mc.quantity_unit" +
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
            GeneralQueries.dropMatchingObservationCodesTempTable();
        }
    }

    public static void createMatchingMedicationCodesTempTable(int extractId, int codeSetId) throws Exception {
        // System.out.println("matching codes");
        // LOG.info("Matching codes medication temp table");

        GeneralQueries.dropMatchingObservationCodesTempTable();

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "create table matching_codes as " +
                    " select " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " from data_generator.cohort_results cr " +
                    " inner join ceg_compass_data.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    "   and csc.code_set_id = :codeSetId" +
                    " where cr.bulked = 0;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

            GeneralQueries.createIndexesOnMatchingObservationCodesTempTable();

        } finally {
            entityManager.close();
        }
    }

    public static void createDeltaMatchingMedicationCodesTempTable(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta matching codes");
        // LOG.info("Delta matching codes medication temp table");

        GeneralQueries.dropMatchingObservationCodesTempTable();

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "create table matching_codes as " +
                    " select " +
                    "  m.id, " +
                    "  m.dmd_id, " +
                    "  m.clinical_effective_date," +
                    "  m.date_precision_id, " +
                    "  m.practitioner_id, " +
                    "  m.organization_id, " +
                    "  m.original_term, " +
                    "  date_format(m.cancellation_date, '%d/%m/%Y') as cancellation_date," +
                    "  m.dose, " +
                    "  m.quantity_value, " +
                    "  m.quantity_unit" +
                    " from data_generator.cohort_results cr " +
                    " inner join ceg_compass_data.medication_statement o on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " inner join rf2.code_set_codes csc on csc.sct_concept_id = m.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
                    "       where e.table_id = 44 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = m.id " +
                    " where cr.bulked = 1;";
            Query query = entityManager.createNativeQuery(sql)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("currentTransactionId", currentTransactionId)
                    .setParameter("maxTransactionId", maxTransactionId);

            entityManager.getTransaction().begin();
            query.executeUpdate();
            entityManager.getTransaction().commit();

            GeneralQueries.createIndexesOnMatchingObservationCodesTempTable();

        } finally {
            entityManager.close();
        }
    }
}