use data_generator;
drop procedure if exists generate_allergy;

DELIMITER //
CREATE PROCEDURE generate_allergy (
	IN col_list VARCHAR(200),
    IN status_code int,
    IN extractId int
)
BEGIN
  SET @sql = CONCAT(
	'CREATE TABLE all_test as 
		SELECT ', col_list, ' 
        FROM pcr.observation o
		join pcr.allergy a on a.concept_id = o.concept_id
	    -- join pcr.code_list cl on cl.concept_id = o.concept_id
	    join data_generator.cohort_results cr on cr.patient_id = o.patient_id
	    and cr.extract_id = ', extractId );-- ,        
		-- 'and a.status_concept_id = ', status_code);
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END//
DELIMITER ;

drop procedure if exists generate_allergy_all_col;

DELIMITER //
CREATE PROCEDURE generate_allergy_all_col (
    IN extractId int,
    IN status_code bigint
)
BEGIN
  CREATE TABLE all_test as 
  SELECT o.* FROM pcr.observation o
  join pcr.allergy a on a.concept_id = o.concept_id
  -- join pcr.code_list cl on cl.concept_id = o.concept_id
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id
  and cr.extract_id = extractId;
  -- and a.status_concept_id = status_code;
END//
DELIMITER ;
