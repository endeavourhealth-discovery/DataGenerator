-- to view the subscriber_transform tables
select * from subscriber_transform.code_set;
select * from subscriber_transform.code_set_codes;
-- where code_set_id = 1;

-- to view the pcr tables
select * from pcr.observation;
select * from pcr.allergy;
select * from pcr.immunisation;
-- select * from pcr.medication;
select * from pcr.medication_amount;
select * from pcr.medication_order;
select * from pcr.medication_statement;
select * from pcr.patient;

-- to test child imms patient.csv file
select * from pcr.patient;

-- to test child imms immunisation.csv file
select * from pcr.immunisation i
join subscriber_transform.code_set_codes c on c.read2_concept_id = i.original_code
where c.code_set_id in (1);

-- to test health check patient.csv file
select * from pcr.patient;

-- to test health check allergy.csv file
select * from pcr.allergy a
join subscriber_transform.code_set_codes c on c.read2_concept_id = a.original_code;

-- to test health check immunisation.csv file
select * from pcr.immunisation i
join subscriber_transform.code_set_codes c on c.read2_concept_id = i.original_code
where c.code_set_id in (1);

-- to test health check medication.csv file
select * from pcr.medication_statement m -- likely to be medication_statement table
join subscriber_transform.code_set_codes c on c.read2_concept_id = m.original_code
where c.code_set_id in (19,25);
                        
-- to test health check observation.csv file
select * from pcr.observation o
join subscriber_transform.code_set_codes c on c.read2_concept_id = o.original_code
where c.code_set_id in (2,3,4,5,6,7,8,9,10,
						11,12,13,14,15,16,17,18,20,
                        21,22,23,24,26,27,28,29,30,
                        31,32,33,34,35,36,37,38,39,40,
                        41,42,43,44,45,46,47,48,49,50,
                        51,52,53,54,55,56,57,58,59,60,
						61,62,63,64,65,66,67);

-- to find out which code sets are relevant to observations (1, 19 & 25 are irrelevant)
-- 1 is Child Immunisations, 19 is Antihypertensive Medications, 25 is Statin Codes
select distinct c.code_set_id from pcr.observation o
join subscriber_transform.code_set_codes c on c.read2_concept_id = o.original_code
where c.code_set_id in (1,2,3,4,5,6,7,8,9,10,
						11,12,13,14,15,16,17,18,19,20,
                        21,22,23,24,25,26,27,28,29,30,
                        31,32,33,34,35,36,37,38,39,40,
                        41,42,43,44,45,46,47,48,49,50,
                        51,52,53,54,55,56,57,58,59,60,
						61,62,63,64,65,66,67);