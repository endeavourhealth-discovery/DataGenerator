CREATE PROCEDURE update_tables_with_bnf 
AS

BEGIN

exec update_bnf_reference '<target>.dbo.medication_statement', <last_updated_date>;
exec update_bnf_reference '<target>.dbo.medication_order', <last_updated_date>;
  
END;
