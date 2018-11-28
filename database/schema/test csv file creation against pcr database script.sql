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