use data_generator;
drop procedure if exists generate_patient;

DELIMITER //
CREATE PROCEDURE generate_patient (
	IN col_list VARCHAR(200),
    IN extractId int
)
BEGIN
  SET @sql = CONCAT(
	'CREATE TABLE pat_test as 
		SELECT ', col_list, ' 
        FROM pcr.patient p
        join data_generator.cohort_results cr on cr.patient_id = p.id
	    and cr.extract_id = ', extractId);
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END//
DELIMITER ;

drop procedure if exists generate_patient_all_col;

DELIMITER //
CREATE PROCEDURE generate_patient_all_col (
    IN extractId int,
    IN codeSetId int,
    IN maxTransactionId int
)
BEGIN
  CREATE TABLE pat_test as   
  SELECT p.* FROM pcr.patient p
  join data_generator.cohort_results cr on cr.patient_id = p.id and cr.extract_id = extractId
  where cr.bulked = 0
  union
  SELECT p.* FROM pcr.patient p
  join data_generator.cohort_results cr on cr.patient_id = p.id and cr.extract_id = extractId
  join data_generator.extract ex on ex.extract_id = cr.extract_id
  join pcr.event_log e on e.item_id = p.id and e.table_id = 8
  where cr.bulked = 1
    and e.id > ex.transaction_id and e.id <= maxTransactionId;
END//
DELIMITER ;
