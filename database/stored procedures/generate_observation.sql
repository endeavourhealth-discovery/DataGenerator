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
    IN extractId int,
    IN codeSetId int
)
BEGIN

  select @max_transaction_id := max(id) from pcr.event_log;

  -- CREATE TABLE obs_test as 
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code and csc.code_set_id = codeSetId
  where cr.bulked = 0
  union
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join data_generator.extract ex on ex.extract_id = cr.extract_id
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code and csc.code_set_id = codeSetId
  join pcr.event_log e on e.item_id = o.id and e.table_name = 'Observation'
  where cr.bulked = 1
    and e.id > ex.transaction_id and e.id <= @max_transaction_id;
    
  update data_generator.cohort_results 
  set bulked = 1 
  where extract_id = extractId;
  
  update data_generator.extract 
  set transaction_id = @max_transaction_id 
  where extract_id = extractId;
  
END//
DELIMITER ;
