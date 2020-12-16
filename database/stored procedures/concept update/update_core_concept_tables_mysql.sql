CREATE PROCEDURE update_tables_with_core_concept_id ()

AS

BEGIN

call update_core_concept_id('<target>.encounter', <last_updated_date>);
call update_core_concept_id('<target>.encounter_event', <last_updated_date>);
call update_core_concept_id('<target>.allergy_intolerance', <last_updated_date>);
call update_core_concept_id('<target>.medication_statement', <last_updated_date>);
call update_core_concept_id('<target>.medication_order', <last_updated_date>);
call update_core_concept_id('<target>.observation', <last_updated_date>);
call update_core_concept_id('<target>.diagnostic_order', <last_updated_date>);
call update_core_concept_id('<target>.procedure_request', <last_updated_date>);
call update_core_concept_id('<target>.referral_request', <last_updated_date>);

END
