drop procedure if exists finalise_extract;

DELIMITER //
CREATE PROCEDURE finalise_extract (
    IN extractId int,
    IN maxTransactionId int
)
BEGIN
    
  update data_generator.cohort_results 
  set bulked = 1 
  where extract_id = extractId;
  
  update data_generator.extract 
  set transaction_id = maxTransactionId 
  where extract_id = extractId;
  
END//
DELIMITER ;