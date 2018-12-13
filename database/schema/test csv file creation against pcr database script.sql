-- To view the subscriber_transform tables
select * from subscriber_transform.code_set;
select * from subscriber_transform.code_set_codes;
-- where code_set_id = 1;

-- To view the pcr tables
select * from pcr.observation;
select * from pcr.allergy;
select * from pcr.immunisation;
-- select * from pcr.medication_amount;
-- select * from pcr.medication_order;
select * from pcr.medication_statement;
select * from pcr.patient;

-- Child Imms Extract

-- To test child imms patient.csv file
select * from pcr.patient;
-- To test child imms immunisation.csv file
select * from pcr.immunisation i
join subscriber_transform.code_set_codes c on c.read2_concept_id = i.original_code
where c.code_set_id in (1);

-- Health Check Extract

-- To test health check patient.csv file
select * from pcr.patient;
-- To test health check allergy.csv file
select * from pcr.allergy;
-- To test health check immunisation.csv file
select * from pcr.immunisation i
join subscriber_transform.code_set_codes c on c.read2_concept_id = i.original_code
where c.code_set_id in (1);
-- To test health check medication.csv file
select * from pcr.medication_statement m 
join subscriber_transform.code_set_codes c on c.read2_concept_id = m.original_code
where c.code_set_id in (19,25);
-- To test health check observation.csv file, code sets 1 & 19,25 & 2,3,4,5 are excluded
-- 1 is Child Immunisations, 19 is Antihypertensive Medications, 25 is Statins
-- 2,3,4,5 are observations for the diabetes extract
select * from pcr.observation o
join subscriber_transform.code_set_codes c on c.read2_concept_id = o.original_code
where c.code_set_id in (6,7,8,9,10,
						11,12,13,14,15,16,17,18,20,
                        21,22,23,24,26,27,28,29,30,
                        31,32,33,34,35,36,37,38,39,40,
                        41,42,43,44,45,46,47,48,49,50,
                        51,52,53,54,55,56,57,58,59,60,
						61,62,63,64,65,66,67);
                        
-- Diabetes Extract

-- To test diabetes patient.csv file
select * from pcr.patient;
-- To test diabetes allergy.csv file
select * from pcr.allergy;
-- To test diabetes immunisation.csv file
select * from pcr.immunisation i
join subscriber_transform.code_set_codes c on c.read2_concept_id = i.original_code
where c.code_set_id in (1);
-- To test diabetes medication.csv file
select * from pcr.medication_statement m 
join subscriber_transform.code_set_codes c on c.read2_concept_id = m.original_code
where c.code_set_id in (19);
-- To test diabetes observation.csv file
-- 2,3,4,5 are Observations for the diabetes extract, 49 is BP Recording, 57 is HbA1c recording
select * from pcr.observation o
join subscriber_transform.code_set_codes c on c.read2_concept_id = o.original_code
where c.code_set_id in (2,3,4,5,49,57);

-- Code Sets

-- To find out which code sets are relevant to observations (1,19,25 are irrelevant)
-- 1 is Child Immunisations, 19 is Antihypertensive Medications, 25 is Statins
select distinct c.code_set_id from pcr.observation o
join subscriber_transform.code_set_codes c on c.read2_concept_id = o.original_code
where c.code_set_id in (1,2,3,4,5,6,7,8,9,10,
						11,12,13,14,15,16,17,18,19,20,
                        21,22,23,24,25,26,27,28,29,30,
                        31,32,33,34,35,36,37,38,39,40,
                        41,42,43,44,45,46,47,48,49,50,
                        51,52,53,54,55,56,57,58,59,60,
						61,62,63,64,65,66,67);