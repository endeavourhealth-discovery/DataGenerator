drop procedure if exists reset_extract;

DELIMITER //
CREATE PROCEDURE reset_extract (
    IN extractId int
)
BEGIN
    
  update data_generator.cohort_results 
  set bulked = 0
  where extract_id = extractId;
  
  update data_generator.extract 
  set transaction_id = 0 
  where extract_id = extractId;
  
 drop table if exists obs_test;
 drop table if exists med_test;
 drop table if exists pat_test;
 drop table if exists imm_test;
 drop table if exists all_test;

END//
DELIMITER ;