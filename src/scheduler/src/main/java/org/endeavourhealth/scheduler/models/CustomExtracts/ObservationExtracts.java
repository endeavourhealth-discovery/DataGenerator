package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObservationExtracts {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationExtracts.class);

    public static List runBulkObservationAllCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk all");
        // LOG.info("Bulk observation all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT DISTINCT " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    "FROM data_generator.cohort_results cr" +
                    " join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
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

    public static List runDeltaObservationAllCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta all");
        // LOG.info("Delta observation all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
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

    public static List runBulkObservationEarliestEachCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk earliest each");
        // LOG.info("Bulk observation earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.observation oo on oo.patient_id = o.patient_id " +
                    "   and oo.original_code = o.original_code " +
                    "   and (o.effective_date < oo.effective_date " +
                    "     or (o.effective_date = oo.effective_date and o.id < oo.id)) " +
                    " where oo.patient_id is null " +
                    "   and cr.bulked = 0 " +
                    "   limit :index, " + size + "; ";
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

    public static List runDeltaObservationEarliestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest each");
        // LOG.info("Delta observation earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.observation oo on oo.patient_id = o.patient_id " +
                    "    and oo.original_code = o.original_code " +
                    "    and (o.effective_date < oo.effective_date " +
                    "       or (o.effective_date = oo.effective_date and o.id < oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
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

    public static List runBulkObservationLatestEachCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk latest each");
        // LOG.info("Bulk observation latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.observation oo on oo.patient_id = o.patient_id " +
                    "   and oo.original_code = o.original_code " +
                    "   and (o.effective_date > oo.effective_date " +
                    "     or (o.effective_date = oo.effective_date and o.id > oo.id)) " +
                    " where oo.patient_id is null " +
                    "   and cr.bulked = 0 " +
                    "   limit :index, " + size + "; ";
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

    public static List runDeltaObservationLatestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest each");
        // LOG.info("Delta observation latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.observation oo on oo.patient_id = o.patient_id " +
                    "    and oo.original_code = o.original_code " +
                    "    and (o.effective_date > oo.effective_date " +
                    "       or (o.effective_date = oo.effective_date and o.id > oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
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

    public static List runBulkObservationLatestCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk latest");
        // LOG.info("Bulk observation latest of all codes");

        // build the temp table to use for subsequent query
        createMatchingObservationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
                    "  mc.effective_date," +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.episodicity_concept_id, " +
                    "  mc.free_text_id, " +
                    "  mc.data_entry_prompt_id, " +
                    "  mc.significance_concept_id, " +
                    "  mc.is_consent, " +
                    "  mc.result_value, " +
                    "  mc.result_value_units, " +
                    "  date_format(mc.result_date, '%d/%m/%Y') as result_date," +
                    "  mc.result_text, " +
                    "  mc.result_concept_id, " +
                    "  mc.reference_range_id, " +
                    "  mc.operator_concept_id " +
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

    public static List runDeltaObservationLatestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest");
        // LOG.info("Delta observation latest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingObservationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
                    "  mc.effective_date," +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.episodicity_concept_id, " +
                    "  mc.free_text_id, " +
                    "  mc.data_entry_prompt_id, " +
                    "  mc.significance_concept_id, " +
                    "  mc.is_consent, " +
                    "  mc.result_value, " +
                    "  mc.result_value_units, " +
                    "  date_format(mc.result_date, '%d/%m/%Y') as result_date," +
                    "  mc.result_text, " +
                    "  mc.result_concept_id, " +
                    "  mc.reference_range_id, " +
                    "  mc.operator_concept_id " +
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

    public static List runBulkObservationEarliestCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk earliest");
        // LOG.info("Bulk observation earliest of all codes");

        // build the temp table to use for subsequent query
        createMatchingObservationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
                    "  mc.effective_date," +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.episodicity_concept_id, " +
                    "  mc.free_text_id, " +
                    "  mc.data_entry_prompt_id, " +
                    "  mc.significance_concept_id, " +
                    "  mc.is_consent, " +
                    "  mc.result_value, " +
                    "  mc.result_value_units, " +
                    "  date_format(mc.result_date, '%d/%m/%Y') as result_date," +
                    "  mc.result_text, " +
                    "  mc.result_concept_id, " +
                    "  mc.reference_range_id, " +
                    "  mc.operator_concept_id " +
                    " from matching_codes mc " +
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

    public static List runDeltaObservationEarliestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest");
        // LOG.info("Delta observation earliest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingObservationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
                    "  mc.effective_date," +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.episodicity_concept_id, " +
                    "  mc.free_text_id, " +
                    "  mc.data_entry_prompt_id, " +
                    "  mc.significance_concept_id, " +
                    "  mc.is_consent, " +
                    "  mc.result_value, " +
                    "  mc.result_value_units, " +
                    "  date_format(mc.result_date, '%d/%m/%Y') as result_date," +
                    "  mc.result_text, " +
                    "  mc.result_concept_id, " +
                    "  mc.reference_range_id, " +
                    "  mc.operator_concept_id " +
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

    public static void createMatchingObservationCodesTempTable(int extractId, int codeSetId) throws Exception {

        // run a drop just in case it has been left due to an error
        GeneralQueries.dropMatchingObservationCodesTempTable();

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "create table matching_codes as " +
                    " select " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
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

    public static void createDeltaMatchingObservationCodesTempTable(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta matching codes");
        // LOG.info("Delta matching codes observation temp table");

        // run a drop just in case it has been left due to an error
        GeneralQueries.dropMatchingObservationCodesTempTable();

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {

            String sql = "create table matching_codes as " +
                    " select " +
                    "  o.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  o.concept_id, " +
                    "  o.effective_date," +
                    "  o.effective_date_precision, " +
                    "  o.effective_practitioner_id, " +
                    "  o.entered_by_practitioner_id, " +
                    "  o.care_activity_id, " +
                    "  o.care_activity_heading_concept_id, " +
                    "  o.owning_organisation_id, " +
                    "  o.is_confidential, " +
                    "  o.original_code, " +
                    "  o.original_term, " +
                    "  o.original_code_scheme, " +
                    "  o.original_system, " +
                    "  o.episodicity_concept_id, " +
                    "  o.free_text_id, " +
                    "  o.data_entry_prompt_id, " +
                    "  o.significance_concept_id, " +
                    "  o.is_consent, " +
                    "  ov.result_value, " +
                    "  ov.result_value_units, " +
                    "  date_format(ov.result_date, '%d/%m/%Y') as result_date," +
                    "  ov.result_text, " +
                    "  ov.result_concept_id, " +
                    "  ov.reference_range_id, " +
                    "  ov.operator_concept_id " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.observation o on o.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = o.id and (pcrm.resource_type in ('Observation','Condition')) " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = o.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.observation_value ov on ov.patient_id = o.patient_id and ov.observation_id = o.id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = o.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
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

            GeneralQueries.createIndexesOnMatchingObservationCodesTempTable();

        } finally {
            entityManager.close();
        }
    }
}