
drop procedure if exists bulk_observation_earliest_each_in_code_set;

DELIMITER //
CREATE PROCEDURE bulk_observation_earliest_each_in_code_set (
    IN extractId int,
    IN codeSetId int
)
BEGIN

  select distinct o.* 
  from data_generator.cohort_results cr 
  inner join pcr.observation o on o.patient_id = cr.patient_id and cr.extract_id = extractId
  inner join subscriber_transform.code_set_codes csc on csc.read2_concept_id = o.original_code 
												and csc.code_set_id = codeSetId
  left join pcr.observation oo on oo.patient_id = o.patient_id
								and oo.original_code = o.original_code
                                and (o.effective_date < oo.effective_date
									or (o.effective_date = oo.effective_date and o.id < oo.id))
  where oo.patient_id is null
    and cr.bulked = 0
  order by o.patient_id;

      
END//
DELIMITER ;
