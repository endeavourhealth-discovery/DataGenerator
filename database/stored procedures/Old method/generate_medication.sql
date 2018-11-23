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
    IN extractId int,
    IN status_code bigint,
    IN codeSetId int,
    IN maxTransactionId int
)
BEGIN
  CREATE TABLE med_test as   
  SELECT ms.* FROM pcr.medication_statement ms
  join data_generator.cohort_results cr on cr.patient_id = ms.patient_id and cr.extract_id = extractId
  where cr.bulked = 0
    and ms.is_active = status_code
  union
  SELECT ms.* FROM pcr.medication_statement ms
  join data_generator.cohort_results cr on cr.patient_id = ms.patient_id and cr.extract_id = extractId
  join data_generator.extract ex on ex.extract_id = cr.extract_id
  join pcr.event_log e on e.item_id = ms.id and e.table_id in ( 44)
  where cr.bulked = 1
    and e.id > ex.transaction_id and e.id <= maxTransactionId
    and ms.is_active = status_code;
END//
DELIMITER ;
