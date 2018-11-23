
drop procedure if exists bulk_observation_latest_in_code_set;

DELIMITER //
CREATE PROCEDURE bulk_observation_latest_in_code_set (
    IN extractId int,
    IN codeSetId int
)
BEGIN

  create table matching_codes as
  select o.* 
  from data_generator.cohort_results cr 
  inner join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = extractId
  inner join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
  and cr.bulked = 0;
                                                
  select distinct mc.* 
  from matching_codes mc 
  left join matching_codes mcoo on mcoo.patient_id = mc.patient_id
                                and (mc.effective_date > mcoo.effective_date
									or (mc.effective_date = mcoo.effective_date and mc.id > mcoo.id))
                                    
  where mcoo.patient_id is null;

  drop table matching_codes;
      
END//
DELIMITER ;
