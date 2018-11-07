use data_generator;
drop procedure if exists generate_observation;

DELIMITER //
CREATE PROCEDURE generate_observation (
	IN col_list VARCHAR(200),
    IN extractId int
)
BEGIN
  SET @sql = CONCAT(
	'CREATE TABLE obs_test as 
		SELECT ', col_list, ' 
        FROM pcr.observation o
	    -- join pcr.code_list cl on cl.concept_id = o.concept_id
	    join data_generator.cohort_results cr on cr.patient_id = o.patient_id
	    and cr.extract_id = ', extractId);
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END//
DELIMITER ;

drop procedure if exists generate_observation_all_col;

DELIMITER //
CREATE PROCEDURE generate_observation_all_col (
    IN extractId int
)
BEGIN
  CREATE TABLE obs_test as 
  SELECT o.* FROM pcr.observation o
  -- join pcr.code_list cl on cl.concept_id = o.concept_id
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id
  and cr.extract_id = extractId;
END//
DELIMITER ;
