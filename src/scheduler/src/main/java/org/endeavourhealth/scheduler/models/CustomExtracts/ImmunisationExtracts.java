package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImmunisationExtracts {

    private static final Logger LOG = LoggerFactory.getLogger(ImmunisationExtracts.class);

    public static List runBulkImmunisationAllCodesQuery(int extractId, int codeSetId, int page, int size) throws Exception {
        // System.out.println("bulk all");
        // LOG.info("Bulk immunisation all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT DISTINCT " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " FROM data_generator.cohort_results cr" +
                    " join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
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

    public static List runDeltaImmunisationAllCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta all");
        // LOG.info("Delta immunisation all codes");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
                    "       where e.table_id = 40 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = i.id " +
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

    public static List runBulkImmunisationEarliestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk earliest each");
        // LOG.info("Bulk immunisation earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.immunisation oo on oo.patient_id = i.patient_id " +
                    "   and oo.original_code = i.original_code " +
                    "   and (i.effective_date < oo.effective_date " +
                    "     or (i.effective_date = oo.effective_date and i.id < oo.id)) " +
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

    public static List runDeltaImmunisationEarliestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest each");
        // LOG.info("Delta immunisation earliest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.immunisation oo on oo.patient_id = i.patient_id " +
                    "    and oo.original_code = i.original_code " +
                    "    and (i.effective_date < oo.effective_date " +
                    "       or (i.effective_date = oo.effective_date and i.id < oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
                    " where e.table_id = 40 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = i.id " +
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

    public static List runBulkImmunisationLatestEachCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk latest each");
        // LOG.info("Bulk immunisation latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select distinct " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
                    " and csc.code_set_id = :codeSetId " +
                    " left join pcr2.immunisation oo on oo.patient_id = i.patient_id " +
                    "   and oo.original_code = i.original_code " +
                    "   and (i.effective_date > oo.effective_date " +
                    "     or (i.effective_date = oo.effective_date and i.id > oo.id)) " +
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

    public static List runDeltaImmunisationLatestEachCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest each");
        // LOG.info("Delta immunisation latest for each code");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT distinct " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " FROM data_generator.cohort_results cr " +
                    " join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " left join pcr2.immunisation oo on oo.patient_id = i.patient_id " +
                    "    and oo.original_code = i.original_code " +
                    "    and (i.effective_date > oo.effective_date " +
                    "       or (i.effective_date = oo.effective_date and i.id > oo.id)) " +
                    " join (select item_id from pcr2.event_log e " +
                    " where e.table_id = 40 " +
                    "   and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "  group by item_id) log on log.item_id = i.id " +
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

    public static List runBulkImmunisationLatestCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk latest");
        // LOG.info("Bulk immunisation latest of all codes");

        // build the temp table to use for subsequent query
        createMatchingImmunisationCodesTempTable(extractId, codeSetId);

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
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.dose, " +
                    "  mc.body_location_concept_id, " +
                    "  mc.method_concept_id, " +
                    "  mc.batch_number, " +
                    "  date_format(mc.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  mc.manufacturer, " +
                    "  mc.dose_ordinal, " +
                    "  mc.doses_required, " +
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

    public static List runDeltaImmunisationLatestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta latest");
        // LOG.info("Delta immunisation latest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingImmunisationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

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
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.dose, " +
                    "  mc.body_location_concept_id, " +
                    "  mc.method_concept_id, " +
                    "  mc.batch_number, " +
                    "  date_format(mc.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  mc.manufacturer, " +
                    "  mc.dose_ordinal, " +
                    "  mc.doses_required, " +
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

    public static List runBulkImmunisationEarliestCodesQuery(int extractId, int codeSetId) throws Exception {
        // System.out.println("bulk earliest");
        // LOG.info("Bulk immunisation earliest of all codes");

        // build the temp table to use for subsequent query
        createMatchingImmunisationCodesTempTable(extractId, codeSetId);

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
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.dose, " +
                    "  mc.body_location_concept_id, " +
                    "  mc.method_concept_id, " +
                    "  mc.batch_number, " +
                    "  date_format(mc.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  mc.manufacturer, " +
                    "  mc.dose_ordinal, " +
                    "  mc.doses_required, " +
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

    public static List runDeltaImmunisationEarliestCodesQuery(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta earliest");
        // LOG.info("Delta immunisation earliest of all codes");

        // build the temp table to use for subsequent query
        createDeltaMatchingImmunisationCodesTempTable(extractId, codeSetId, currentTransactionId, maxTransactionId);

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
                    "  mc.status_concept_id, " +
                    "  mc.is_confidential, " +
                    "  mc.original_code, " +
                    "  mc.original_term, " +
                    "  mc.original_code_scheme, " +
                    "  mc.original_system, " +
                    "  mc.dose, " +
                    "  mc.body_location_concept_id, " +
                    "  mc.method_concept_id, " +
                    "  mc.batch_number, " +
                    "  date_format(mc.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  mc.manufacturer, " +
                    "  mc.dose_ordinal, " +
                    "  mc.doses_required, " +
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

    public static void createMatchingImmunisationCodesTempTable(int extractId, int codeSetId) throws Exception {
        // System.out.println("matching codes");
        // LOG.info("Matching codes immunisation temp table");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "create table matching_codes as " +
                    " select " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
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

    public static void createDeltaMatchingImmunisationCodesTempTable(int extractId, int codeSetId, Long currentTransactionId, Long maxTransactionId) throws Exception {
        // System.out.println("delta matching codes");
        // LOG.info("Delta matching codes immunisation temp table");

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "create table matching_codes as " +
                    " select " +
                    "  i.id, " +
                    "  pcrm.resource_id, " +
                    "  pcrmpat.resource_id as patient_id, " +
                    "  i.concept_id, " +
                    "  i.effective_date, " +
                    "  i.effective_date_precision, " +
                    "  i.effective_practitioner_id, " +
                    "  i.entered_by_practitioner_id, " +
                    "  i.care_activity_id, " +
                    "  i.care_activity_heading_concept_id, " +
                    "  i.owning_organisation_id, " +
                    "  i.status_concept_id, " +
                    "  i.is_confidential, " +
                    "  i.original_code, " +
                    "  i.original_term, " +
                    "  i.original_code_scheme, " +
                    "  i.original_system, " +
                    "  i.dose, " +
                    "  i.body_location_concept_id, " +
                    "  i.method_concept_id, " +
                    "  i.batch_number, " +
                    "  date_format(i.expiry_date, '%d/%m/%Y') as expiry_date," +
                    "  i.manufacturer, " +
                    "  i.dose_ordinal, " +
                    "  i.doses_required, " +
                    "  i.is_consent" +
                    " from data_generator.cohort_results cr " +
                    " inner join pcr2.immunisation i on i.patient_id = cr.patient_id and cr.extract_id = :extractId " +
                    " join subscriber_transform_pcr.pcr_id_map pcrm on pcrm.pcr_id = i.id and pcrm.resource_type = 'Immunization' " +
                    " join subscriber_transform_pcr.pcr_id_map pcrmpat on pcrmpat.pcr_id = i.patient_id and pcrmpat.resource_type = 'Patient' " +
                    " inner join subscriber_transform_pcr.code_set_codes csc on csc.read2_concept_id = i.original_code " +
                    "   and csc.code_set_id = :codeSetId " +
                    " join (select item_id from pcr2.event_log e " +
                    "       where e.table_id = 40 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "       group by item_id) log on log.item_id = i.id " +
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