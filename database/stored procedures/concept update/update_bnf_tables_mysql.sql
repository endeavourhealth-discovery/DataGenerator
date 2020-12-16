CREATE PROCEDURE update_tables_with_bnf ()

BEGIN

call update_bnf_reference('<target>.medication_statement', <last_updated_date>);
call update_bnf_reference('<target>.medication_order', <last_updated_date>);

END
