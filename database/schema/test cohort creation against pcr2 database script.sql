-- All Patients
-- Cohort Count Test

-- To count in Cohort
select * from data_generator.cohort_results where extract_id = 1;

-- To count in PCR2
select distinct p.id
from pcr2.patient p
join pcr2.gp_registration_status reg on reg.patient_id = p.id
and
(reg.gp_registration_status_concept_id = 2 and reg.is_current = true)
and
p.date_of_death is null;


-- Diabetes Aged 12+ Patient
-- Cohort Count Test

-- To count in Cohort
select * from data_generator.cohort_results where extract_id = 2;

-- To count in PCR2
select count(distinct(p.nhs_number)) as 'Diabetics 12+ Patients Count' from pcr2.observation o
join pcr2.organisation org on org.id = o.owning_organisation_id
join pcr2.patient p on p.id = o.patient_id
join pcr2.gp_registration_status reg on reg.patient_id = p.id
-- where
-- org.ods_code = _odscode
where
o.original_code in (select read2_concept_id from subscriber_transform_pcr.code_set_codes
						where code_set_id in (3,4,5))
and
p.date_of_birth <= DATE(NOW() - INTERVAL 12 year)
and
(reg.gp_registration_status_concept_id = 2 and reg.is_current = true)
and
p.date_of_death is null;


-- Asthma Patients
-- Cohort Count Test

-- Patients who have an Asthma diagnosis code (68) and have
-- been given Asthma medication (69) during the last 12 months

-- To count in Cohort
select * from data_generator.cohort_results where extract_id = 3;

-- To count in PCR2
select count(distinct(p.nhs_number)) as 'Asthma Patients' from pcr2.observation o
join pcr2.organisation org on org.id = o.owning_organisation_id
join pcr2.patient p on p.id = o.patient_id
join pcr2.medication_statement ms on ms.patient_id = p.id
-- join pcr2.medication_order mo on mo.medication_statement_id = ms.id
join pcr2.gp_registration_status reg on reg.patient_id = p.id
join subscriber_transform_pcr.code_set_codes csc1 on csc1.read2_concept_id = o.original_code
join subscriber_transform_pcr.code_set_codes csc2 on csc2.sct_concept_id = ms.original_code
where
-- org.ods_code =  _odscode
-- and
(reg.gp_registration_status_concept_id = 2 and reg.is_current = true)
and
p.date_of_death is null
and
(csc1.code_set_id = 68 and csc2.code_set_id = 69);
-- and
-- mo.effective_date >= DATE(NOW() - INTERVAL 12 MONTH);

-- Just to count diagnosis, rather than medication as well
select count(distinct(p.nhs_number)) as 'Asthma Patients Count' from pcr2.observation o
join pcr2.organisation org on org.id = o.owning_organisation_id
join pcr2.patient p on p.id = o.patient_id
join pcr2.gp_registration_status reg on reg.patient_id = p.id
-- where
-- org.ods_code = _odscode
where
o.original_code in (select read2_concept_id from subscriber_transform_pcr.code_set_codes
						where code_set_id in (68))
and
(reg.gp_registration_status_concept_id = 2 and reg.is_current = true)
and
p.date_of_death is null;


-- Health Check
-- Cohort Count Test

-- To count in Cohort
select * from data_generator.cohort_results where extract_id = 4;

-- To count in PCR2 (this query takes about 3/4 of an hour to run!)
select count(distinct(p.nhs_number)) as 'Health Check Patients Count' from pcr2.observation o
join pcr2.organisation org on org.id = o.owning_organisation_id
join pcr2.patient p on p.id = o.patient_id
join pcr2.medication_statement ms on ms.patient_id = p.id
-- join pcr2.medication_order mo on mo.medication_statement_id = ms.id
join pcr2.gp_registration_status reg on reg.patient_id = p.id
join subscriber_transform_pcr.code_set_codes csc1 on csc1.read2_concept_id = o.original_code
join subscriber_transform_pcr.code_set_codes csc2 on csc2.sct_concept_id = ms.original_code
where
-- org.ods_code =  _odscode
-- and
(reg.gp_registration_status_concept_id = 2 and reg.is_current = true)
and
p.date_of_death is null
and
((csc1.code_set_id in (8,14,11,23,17,9,22,15,16,20,21)) or (csc2.code_set_id in (25)));
-- and
-- mo.effective_date >= DATE(NOW() - INTERVAL 12 MONTH);

-- To count either patients with observations to exclude,
-- or, alternatively, those with statin prescriptions to exclude
select count(distinct(p.nhs_number)) as 'Health Check 40-74 Count' from pcr2.observation o
join pcr2.organisation org on org.id = o.owning_organisation_id
join pcr2.patient p on p.id = o.patient_id
-- join pcr2.medication_statement ms on ms.patient_id = p.id
join pcr2.gp_registration_status reg on reg.patient_id = p.id
-- where
-- org.ods_code = _odscode
where
o.original_code in (select read2_concept_id from subscriber_transform_pcr.code_set_codes
	 					where code_set_id in (8,9,11,14,15,16,17,20,21,22,23))
-- where
-- ms.original_code in (select sct_concept_id from subscriber_transform_pcr.code_set_codes
						-- where code_set_id in (25))
--
-- For just an age range count start here!
-- select distinct p.id
-- from pcr2.patient p
-- join pcr2.gp_registration_status reg on reg.patient_id = p.id
--
and
p.date_of_birth <= DATE(NOW() - INTERVAL 40 year)
and
p.date_of_birth >= DATE(NOW() - INTERVAL 74 year)
and
(reg.gp_registration_status_concept_id = 2 and reg.is_current = true)
and
p.date_of_death is null;

-- Child Immunisations
-- Cohort Count Test

-- To count in Cohort
select * from data_generator.cohort_results where extract_id = 5;

-- To count in PCR2
select distinct p.id
from pcr2.patient p
join pcr2.gp_registration_status reg on reg.patient_id = p.id
and
(reg.gp_registration_status_concept_id = 2 and reg.is_current = true)
and
p.date_of_death is null
and p.date_of_birth >= DATE(NOW() - INTERVAL 20 year);