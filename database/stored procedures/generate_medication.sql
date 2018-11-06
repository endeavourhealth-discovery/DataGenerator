use data_generator;
drop procedure if exists generate_medication;

DELIMITER //
CREATE PROCEDURE generate_medication (
	IN col_list VARCHAR(200),
    IN medication_status int
)
BEGIN
  SET @sql = CONCAT('CREATE TABLE med_test as SELECT ', col_list, ' FROM pcr.medication_statement where is_active = 1');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END//
DELIMITER ;
