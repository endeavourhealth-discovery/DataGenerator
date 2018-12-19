package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MedicationExtracts {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationExtracts.class);

    public static List runBulkMedicationAllCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk all");
        // LOG.info("Bulk medication all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT " +
                    "  m.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units" +
                    " FROM data_generator.cohort_results cr" +
                    " join pcr2.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = m.id and pcrm.resource_type = 'MedicationStatement' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = m.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
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

    public static List runDeltaMedicationAllCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta all");
        // LOG.info("Delta medication all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  m.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = m.id and pcrm.resource_type = 'MedicationStatement' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = m.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id" +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
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

    public static List runBulkMedicationEarliestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk earliest each");
        // LOG.info("Bulk medication earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  m.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = m.id and pcrm.resource_type = 'MedicationStatement' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = m.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.medication_statement oo on oo.patient_id = m.patient_id " +
                    "   and oo.original_code = m.original_code " +
                    "   and (m.effective_date < oo.effective_date " +
                    "     or (m.effective_date = oo.effective_date and m.id < oo.id)) " +
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

    public static List runDeltaMedicationEarliestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest each");
        // LOG.info("Delta medication earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  m.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = m.id and pcrm.resource_type = 'MedicationStatement' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = m.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.medication_statement oo on oo.patient_id = m.patient_id " +
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

    public static List runBulkMedicationLatestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk latest each");
        // LOG.info("Bulk medication latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  m.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = m.id and pcrm.resource_type = 'MedicationStatement' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = m.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.medication_statement oo on oo.patient_id = m.patient_id " +
                    "   and oo.original_code = m.original_code " +
                    "   and (m.effective_date > oo.effective_date " +
                    "     or (m.effective_date = oo.effective_date and m.id > oo.id)) " +
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

    public static List runDeltaMedicationLatestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest each");
        // LOG.info("Delta medication latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  m.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = m.id and pcrm.resource_type = 'MedicationStatement' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = m.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.medication_statement oo on oo.patient_id = m.patient_id " +
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

    public static List runBulkMedicationLatestCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk latest");
        // LOG.info("Bulk medication latest of all codes");

        // build the temp table to use for subsequent query
        createMatchingMedicationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.drug_concept_id, " +
                    "  mc.effective_date, " +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.type_concept_id, " +
                    "  mc.medication_amount_id, " +
                    "  mc.issues_authorised, " +
                    "  mc.review_date, " +
                    "  mc.course_length_per_issue_days, " +
                    "  mc.patient_instructions_free_text_id, " +
                    "  mc.pharmacy_instructions_free_text_id, " +
                    "  mc.is_active, " +
                    "  mc.end_date, " +
                    "  mc.end_reason_concept_id, " +
                    "  mc.end_reason_free_text_id, " +
                    "  mc.issues, " +
                    "  mc.is_consent, " +
                    "  mc.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
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

    public static List runDeltaMedicationLatestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest");
        // LOG.info("Delta medication latest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingMedicationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.drug_concept_id, " +
                    "  mc.effective_date, " +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.type_concept_id, " +
                    "  mc.medication_amount_id, " +
                    "  mc.issues_authorised, " +
                    "  mc.review_date, " +
                    "  mc.course_length_per_issue_days, " +
                    "  mc.patient_instructions_free_text_id, " +
                    "  mc.pharmacy_instructions_free_text_id, " +
                    "  mc.is_active, " +
                    "  mc.end_date, " +
                    "  mc.end_reason_concept_id, " +
                    "  mc.end_reason_free_text_id, " +
                    "  mc.issues, " +
                    "  mc.is_consent, " +
                    "  mc.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
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

    public static List runBulkMedicationEarliestCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk earliest");
        // LOG.info("Bulk medication earliest of all codes");

        // build the temp table to use for subsequent query
        createMatchingMedicationCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.drug_concept_id, " +
                    "  mc.effective_date, " +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.type_concept_id, " +
                    "  mc.medication_amount_id, " +
                    "  mc.issues_authorised, " +
                    "  mc.review_date, " +
                    "  mc.course_length_per_issue_days, " +
                    "  mc.patient_instructions_free_text_id, " +
                    "  mc.pharmacy_instructions_free_text_id, " +
                    "  mc.is_active, " +
                    "  mc.end_date, " +
                    "  mc.end_reason_concept_id, " +
                    "  mc.end_reason_free_text_id, " +
                    "  mc.issues, " +
                    "  mc.is_consent, " +
                    "  mc.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
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

    public static List runDeltaMedicationEarliestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest");
        // LOG.info("Delta medication earliest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingMedicationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.drug_concept_id, " +
                    "  mc.effective_date, " +
                    "  mc.effective_date_precision, " +
                    "  mc.effective_practitioner_id, " +
                    "  mc.entered_by_practitioner_id, " +
                    "  mc.care_activity_id, " +
                    "  mc.care_activity_heading_concept_id, " +
                    "  mc.owning_organisation_id, " +
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.type_concept_id, " +
                    "  mc.medication_amount_id, " +
                    "  mc.issues_authorised, " +
                    "  mc.review_date, " +
                    "  mc.course_length_per_issue_days, " +
                    "  mc.patient_instructions_free_text_id, " +
                    "  mc.pharmacy_instructions_free_text_id, " +
                    "  mc.is_active, " +
                    "  mc.end_date, " +
                    "  mc.end_reason_concept_id, " +
                    "  mc.end_reason_free_text_id, " +
                    "  mc.issues, " +
                    "  mc.is_consent, " +
                    "  mc.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
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
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.medication_statement m on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
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
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  m.drug_concept_id, " +
                    "  m.effective_date, " +
                    "  m.effective_date_precision, " +
                    "  m.effective_practitioner_id, " +
                    "  m.entered_by_practitioner_id, " +
                    "  m.care_activity_id, " +
                    "  m.care_activity_heading_concept_id, " +
                    "  m.owning_organisation_id, " +
                    "  m.status_concept_id, " +
                    "  m.is_confidential, " +
                    "  m.original_code, " +
                    "  m.original_term, " +
                    "  m.original_code_scheme, " +
                    "  m.original_system, " +
                    "  m.type_concept_id, " +
                    "  m.medication_amount_id, " +
                    "  m.issues_authorised, " +
                    "  m.review_date, " +
                    "  m.course_length_per_issue_days, " +
                    "  m.patient_instructions_free_text_id, " +
                    "  m.pharmacy_instructions_free_text_id, " +
                    "  m.is_active, " +
                    "  m.end_date, " +
                    "  m.end_reason_concept_id, " +
                    "  m.end_reason_free_text_id, " +
                    "  m.issues, " +
                    "  m.is_consent, " +
                    "  ma.dose, " +
                    "  ma.quantity_value, " +
                    "  ma.quantity_units " +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.medication_statement o on m.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = m.id and pcrm.resource_type = 'MedicationStatement' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = m.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " left outer join pcr2.medication_amount ma on ma.id = m.medication_amount_id " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = m.original_code " +
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