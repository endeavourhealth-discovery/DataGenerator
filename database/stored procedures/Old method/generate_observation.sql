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
    IN codeSetId int,
    IN maxTransactionId bigint
)
BEGIN

  -- get the latest of each code in the dataset for the patient for codes defined as latest only
  create table latest_codes as
  select distinct o.id -- min(o.effective_date) as effective_date, o.original_code, o.patient_id
  from pcr.observation o 
  left join pcr.observation oo on oo.patient_id = o.patient_id
								and oo.original_code = o.original_code
                                and (o.effective_date < oo.effective_date
									or (o.effective_date = oo.effective_date and o.id < oo.id))
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
                                                and csc.extract_type = 2
  where oo.patient_id is null;
  
  
  CREATE INDEX ix_latest_codes ON latest_codes (id);
                                                
   /*                                             
  select max(o.effective_date) as effective_date, o.original_code, o.patient_id
  from pcr.observation o 
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
                                                and csc.extract_type = 2
   group by o.patient_id, o.original_code;
  
  */
  -- get the earliest of each code in the dataset for the patient for codes defined as earliest only
  create table earliest_codes as
  select distinct o.id -- min(o.effective_date) as effective_date, o.original_code, o.patient_id
  from pcr.observation o 
  left join pcr.observation oo on oo.patient_id = o.patient_id
								and oo.original_code = o.original_code
                                and (o.effective_date > oo.effective_date
									or (o.effective_date = oo.effective_date and o.id < oo.id))
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
                                                and csc.extract_type = 1
  where oo.patient_id is null;
  
  CREATE INDEX ix_earliest_codes ON earliest_codes (id);
  
  /*
  select min(o.effective_date) as effective_date, o.original_code, o.patient_id
  from pcr.observation o 
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
                                                and csc.extract_type = 1
							
   group by o.patient_id, o.original_code;
   */
  CREATE TABLE obs_test as 
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
                                                and csc.extract_type = 0 -- all codes
  where cr.bulked = 0
  union
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join earliest_codes ec on o.patient_id = ec.id  -- earliest codes only
  where cr.bulked = 0
  union  
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join latest_codes ec on o.id = ec.id  -- latest codes only
  where cr.bulked = 0
  union
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join data_generator.extract ex on ex.extract_id = cr.extract_id
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
                                                and csc.extract_type = 0 -- all codes                                                
  join pcr.event_log e on e.item_id = o.id and e.table_id = 32
  where cr.bulked = 1
    and e.id > ex.transaction_id and e.id <= maxTransactionId
  union
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join data_generator.extract ex on ex.extract_id = cr.extract_id
  join earliest_codes ec on o.id = ec.id  -- earliest codes only  
  join pcr.event_log e on e.item_id = ec.id and e.table_id = 32
  where cr.bulked = 1
    and e.id > ex.transaction_id and e.id <= maxTransactionId
  union
  SELECT o.* FROM pcr.observation o
  join data_generator.cohort_results cr on cr.patient_id = o.patient_id and cr.extract_id = extractId
  join data_generator.extract ex on ex.extract_id = cr.extract_id
  join latest_codes ec on o.id = ec.id  -- latest codes only  
  join pcr.event_log e on e.item_id = ec.id and e.table_id = 32
  where cr.bulked = 1
    and e.id > ex.transaction_id and e.id <= maxTransactionId;
    
  drop table if exists earliest_codes;
  drop table if exists latest_codes;
      
END//
DELIMITER ;
