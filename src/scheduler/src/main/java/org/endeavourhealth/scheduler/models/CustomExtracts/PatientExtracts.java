package org.endeavourhealth.scheduler.models.CustomExtracts;

import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientExtracts {

    private static final Logger LOG = LoggerFactory.getLogger(PatientExtracts.class);

    public static List runBulkPatientExtract(int extractId) throws Exception {
        // System.out.println("bulk all patients");
        // LOG.info("Bulk all patients");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT " +
                    " p.id," +
                    " p.organisation_id," +
                    " p.nhs_number," +
                    " p.nhs_number_verification_concept_id," +
                    " p.date_of_birth," +
                    " p.date_of_death," +
                    " p.gender_concept_id," +
                    " p.usual_practitioner_id," +
                    " p.care_provider_id," +
                    " p.entered_by_practitioner_id," +
                    " p.title," +
                    " p.first_name," +
                    " p.middle_names," +
                    " p.last_name," +
                    " p.previous_last_name," +
                    " p.home_address_id," +
                    " p.is_spine_sensitive," +
                    " p.ethnic_code," +
                    " a.address_line_1," +
                    " a.address_line_2," +
                    " a.address_line_3," +
                    " a.address_line_4," +
                    " a.postcode," +
                    " a.uprn," +
                    " a.approximation_concept_id, " +
                    " a.property_type_concept_id, " +
                    " org.ods_code, " +
                    " org.name as organisation_name, " +
                    " grs.effective_date as registered_date, " +
                    " pid.value as usual_practitioner_number " +
                    " FROM pcr2.patient p " +
                    " left outer join pcr2.patient_address pa on pa.address_id = p.home_address_id " +
                    " left outer join pcr2.address a on a.id = pa.address_id " +
                    " left outer join pcr2.organisation org on org.id = p.organisation_id " +
                    " left outer join pcr2.gp_registration_status grs on grs.patient_id = p.id " +
                    " left outer join pcr2.practitioner_identifier pid on pid.practitioner_id = p.usual_practitioner_id " +
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
        // System.out.println("delta all patients");
        // LOG.info("Delta all patients");
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "SELECT " +
                    " p.id," +
                    " p.organisation_id," +
                    " p.nhs_number," +
                    " p.nhs_number_verification_concept_id," +
                    " p.date_of_birth," +
                    " p.date_of_death," +
                    " p.gender_concept_id," +
                    " p.usual_practitioner_id," +
                    " p.care_provider_id," +
                    " p.entered_by_practitioner_id," +
                    " p.title," +
                    " p.first_name," +
                    " p.middle_names," +
                    " p.last_name," +
                    " p.previous_last_name," +
                    " p.home_address_id," +
                    " p.is_spine_sensitive," +
                    " p.ethnic_code," +
                    " a.address_line_1," +
                    " a.address_line_2," +
                    " a.address_line_3," +
                    " a.address_line_4," +
                    " a.postcode," +
                    " a.uprn," +
                    " a.approximation_concept_id, " +
                    " a.property_type_concept_id, " +
                    " org.ods_code, " +
                    " org.name as organisation_name, " +
                    " grs.effective_date as registered_date, " +
                    " pid.value as usual_practitioner_number" +
                    " FROM pcr2.patient p " +
                    " left outer join pcr2.patient_address pa on pa.address_id = p.home_address_id " +
                    " left outer join pcr2.address a on a.id = pa.address_id " +
                    " left outer join pcr2.organisation org on org.id = p.organisation_id " +
                    " left outer join pcr2.gp_registration_status grs on grs.patient_id = p.id" +
                    " left outer join pcr2.practitioner_identifier pid on pid.practitioner_id = p.usual_practitioner_id" +
                    " join data_generator.cohort_results cr on cr.patient_id = p.id and cr.extract_id = :extractId " +
                    " join (select item_id from pcr2.event_log e " +
                    "       where e.table_id = 8 " +
                    "         and e.id > :currentTransactionId and e.id <= :maxTransactionId " +
                    "        group by item_id) log on log.item_id = p.id " +
                    " where cr.bulked = 1;";
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