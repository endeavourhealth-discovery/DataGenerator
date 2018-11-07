use data_generator;
drop procedure if exists generate_medication;

DELIMITER //
CREATE PROCEDURE generate_medication (
	IN col_list VARCHAR(200),
    IN status_code int,
    IN extractId int
)
BEGIN
  SET @sql = CONCAT(
	'CREATE TABLE med_test as 
		SELECT ', col_list, ' 
        FROM pcr.medication_statement ms
        join data_generator.cohort_results cr on cr.patient_id = ms.patient_id
	    where ms.is_active = ', status_code, '
	    and cr.extract_id = ', extractId);
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END//
DELIMITER ;

drop procedure if exists generate_medication_all_col;

DELIMITER //
CREATE PROCEDURE generate_medication_all_col (
    IN status_code int,
    IN extractId int
)
BEGIN
  CREATE TABLE med_test as 
  SELECT ms.* FROM pcr.medication_statement ms
  join data_generator.cohort_results cr on cr.patient_id = ms.patient_id
  where ms.is_active = status_code
  and cr.extract_id = extractId;
END//
DELIMITER ;
