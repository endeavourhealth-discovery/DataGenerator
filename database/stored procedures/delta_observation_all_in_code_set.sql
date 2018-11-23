drop procedure if exists delta_observation_all_in_code_set;

DELIMITER //
CREATE PROCEDURE delta_observation_all_in_code_set (
    IN extractId int,
    IN codeSetId int,
    IN maxTransactionId bigint
)
BEGIN

  select @current_transaction_id := transaction_id from data_generator.extract where extract_id = extractId;

  SELECT distinct o.* 
  FROM data_generator.cohort_results cr 
  join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = extractId
  join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
  join (select item_id from pcr.event_log e 
			where e.table_id = 32 
            and e.id > @current_transaction_id and e.id <= maxTransactionId
            group by item_id) log on log.item_id = o.id 
  where cr.bulked = 1;

      
END//
DELIMITER ;
