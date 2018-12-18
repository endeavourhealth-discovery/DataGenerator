package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllergyExtracts {

    private static final Logger LOG = LoggerFactory.getLogger(AllergyExtracts.class);

    public static List runBulkAllergyAllCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk all");
        // LOG.info("Bulk allergy all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " FROM data_generator.cohort_results cr" +
                    " join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
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

    public static List runDeltaAllergyAllCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta all");
        // LOG.info("Delta allergy all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
                    "       where e.table_id = 41 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = a.id " +
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

    public static List runBulkAllergyEarliestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk earliest each");
        // LOG.info("Bulk allergy earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.allergy oo on oo.patient_id = a.patient_id " +
                    "   and oo.original_code = a.original_code " +
                    "   and (a.effective_date < oo.effective_date " +
                    "     or (a.effective_date = oo.effective_date and a.id < oo.id)) " +
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

    public static List runDeltaAllergyEarliestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest each");
        // LOG.info("Delta allergy earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.allergy oo on oo.patient_id = a.patient_id " +
                    "    and oo.original_code = a.original_code " +
                    "    and (a.effective_date < oo.effective_date " +
                    "       or (a.effective_date = oo.effective_date and a.id < oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
                    " where e.table_id = 41 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = a.id " +
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

    public static List runBulkAllergyLatestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk latest each");
        // LOG.info("Bulk allergy latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.allergy oo on oo.patient_id = a.patient_id " +
                    "   and oo.original_code = a.original_code " +
                    "   and (a.effective_date > oo.effective_date " +
                    "     or (a.effective_date = oo.effective_date and a.id > oo.id)) " +
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

    public static List runDeltaAllergyLatestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest each");
        // LOG.info("Delta allergy latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.allergy oo on oo.patient_id = a.patient_id " +
                    "    and oo.original_code = a.original_code " +
                    "    and (a.effective_date > oo.effective_date " +
                    "       or (a.effective_date = oo.effective_date and a.id > oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
                    " where e.table_id = 41 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = a.id " +
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

    public static List runBulkAllergyLatestCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk latest");
        // LOG.info("Bulk allergy latest of all codes");

        // build the temp table to use for subsequent query
        createMatchingAllergyCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
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
                    "  mc.substance_concept_id, " +
                    "  mc.manifestation_concept_id, " +
                    "  mc.manifestation_free_text_id, " +
                    "  mc.is_consent" +
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

    public static List runDeltaAllergyLatestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest");
        // LOG.info("Delta allergy latest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingAllergyCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
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
                    "  mc.substance_concept_id, " +
                    "  mc.manifestation_concept_id, " +
                    "  mc.manifestation_free_text_id, " +
                    "  mc.is_consent" +
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

    public static List runBulkAllergyEarliestCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk earliest");
        // LOG.info("Bulk allergy earliest of all codes");

        // build the temp table to use for subsequent query
        createMatchingAllergyCodesTempTable(extractId, codeSetId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
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
                    "  mc.substance_concept_id, " +
                    "  mc.manifestation_concept_id, " +
                    "  mc.manifestation_free_text_id, " +
                    "  mc.is_consent" +
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

    public static List runDeltaAllergyEarliestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest");
        // LOG.info("Delta allergy earliest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingAllergyCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  mc.id, " +
                    "  mc.resource_id, " +
                    "  mc.patient_id, " +
                    "  mc.concept_id, " +
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
                    "  mc.substance_concept_id, " +
                    "  mc.manifestation_concept_id, " +
                    "  mc.manifestation_free_text_id, " +
                    "  mc.is_consent" +
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

    public static void createMatchingAllergyCodesTempTable(int extractId, int codeSetId) throws Exception {
        // System.out.println("matching codes");
        // LOG.info("Matching codes allergy temp table");

        GeneralQueries.dropMatchingObservationCodesTempTable();

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "create table matching_codes as " +
                    " select " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
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

    public static void createDeltaMatchingAllergyCodesTempTable(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta matching codes");
        // LOG.info("Delta matching codes allergy temp table");

        GeneralQueries.dropMatchingObservationCodesTempTable();

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "create table matching_codes as " +
                    " select " +
                    "  a.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  a.concept_id, " +
                    "  a.effective_date, " +
                    "  a.effective_date_precision, " +
                    "  a.effective_practitioner_id, " +
                    "  a.entered_by_practitioner_id, " +
                    "  a.care_activity_id, " +
                    "  a.care_activity_heading_concept_id, " +
                    "  a.owning_organisation_id, " +
                    "  a.status_concept_id, " +
                    "  a.is_confidential, " +
                    "  a.original_code, " +
                    "  a.original_term, " +
                    "  a.original_code_scheme, " +
                    "  a.original_system, " +
                    "  a.substance_concept_id, " +
                    "  a.manifestation_concept_id, " +
                    "  a.manifestation_free_text_id, " +
                    "  a.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.allergy a on a.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = a.id and pcrm.resource_type = 'Allergy' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = a.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = a.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
                    "       where e.table_id = 41 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = a.id " +
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