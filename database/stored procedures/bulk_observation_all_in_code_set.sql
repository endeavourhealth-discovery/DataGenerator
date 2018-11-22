drop procedure if exists bulk_observation_all_in_code_set;

DELIMITER //
CREATE PROCEDURE bulk_observation_all_in_code_set (
    IN extractId int,
    IN codeSetId int
)
BEGIN

  SELECT o.* FROM data_generator.cohort_results cr 
  join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
  where cr.bulked = 0;

      
END//
DELIMITER ;
