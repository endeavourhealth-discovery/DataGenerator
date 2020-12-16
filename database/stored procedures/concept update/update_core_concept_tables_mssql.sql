CREATE PROCEDURE update_tables_with_core_concept_id 
AS

BEGIN

exec update_core_concept_id '<target>.dbo.encounter', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.encounter_event', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.allergy_intolerance', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.medication_statement', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.medication_order', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.observation', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.diagnostic_order', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.procedure_request', <last_updated_date>;
exec update_core_concept_id '<target>.dbo.referral_request', <last_updated_date>;
  
END;
