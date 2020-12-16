CREATE PROCEDURE update_bnf_reference(
    @tableName AS NVARCHAR(128),
    @last_updated_date AS DATETIME
) 
AS

DECLARE  @i     BIGINT;
DECLARE  @cnt   BIGINT;
DECLARE  @table NVARCHAR(128);
DECLARE  @sql   NVARCHAR(MAX);
DECLARE  @sql1  NVARCHAR(MAX);

BEGIN

  SET @i = 0;
  SET @table = @tableName;

  DROP TABLE IF EXISTS dbo.qry_tmp;

  CREATE TABLE dbo.qry_tmp( 
   snomed_code      NVARCHAR(20),
   bnf_chapter_code NVARCHAR(6), 
   dt_last_updated  DATETIME
  );

  CREATE INDEX qry_snomed_code_ix ON dbo.qry_tmp(snomed_code);
  CREATE INDEX qry_bnf_chapter_code_ix ON dbo.qry_tmp(bnf_chapter_code);

  WITH qry AS (
    SELECT s.snomed_code, SUBSTRING(s.bnf_chapter_code, 1, 6) as bnf_chapter_code , s.dt_last_updated
    FROM dbo.snomed_to_bnf_chapter_lookup s
    WHERE s.dt_last_updated > @last_updated_date
  )
  INSERT INTO dbo.qry_tmp
  SELECT * FROM qry;

  DROP TABLE IF EXISTS dbo.qry_tmp_2;

  CREATE TABLE dbo.qry_tmp_2( 
   id                BIGINT,
   snomed_code       NVARCHAR(20), 
   bnf_chapter_code  NVARCHAR(6),
   row_id            BIGINT NOT NULL PRIMARY KEY
  );

  SET @sql1 = N'WITH qry2 AS ( '+
	'SELECT t.id id, c.code snomed_code, substring(r.bnf_chapter_code,1,6) bnf_chapter_code, ROW_NUMBER() OVER(ORDER BY q.snomed_code) AS row_id ' +
	'FROM db_lookup.dbo.concept c INNER JOIN ' + @table + ' t ON t.non_core_concept_id = c.dbid ' +
	'INNER JOIN db_lookup.dbo.snomed_to_bnf_chapter_lookup r  ON r.snomed_code = c.code ' +
	'INNER JOIN dbo.qry_tmp q ON q.snomed_code = c.code ' +
	'WHERE ((t.bnf_reference <> q.bnf_chapter_code AND t.bnf_reference IS NOT NULL) OR t.bnf_reference IS NULL) and c.scheme = 71 ' +
  ') '+
  'INSERT INTO dbo.qry_tmp_2 '+
  'SELECT * FROM qry2; ';

  EXEC sp_executesql @sql1; 

  SELECT @cnt = COUNT(*) FROM dbo.qry_tmp_2;
 
  WHILE (@i < @cnt)

  BEGIN

  SET @sql = N'UPDATE t '+ 
      'SET t.bnf_reference = q.bnf_chapter_code '+
      'FROM ' + @table + ' t INNER JOIN dbo.qry_tmp_2 q ON q.id = t.id '+
      'WHERE q.row_id > '+ CONVERT(VARCHAR(20), @i) + ' AND q.row_id <= ' +  CONVERT(VARCHAR(20), @i)  + ' + 1000;'; 
  
  EXEC sp_executesql @sql;
 
  SELECT @i = @i + 1000; 

  END;

  DROP TABLE IF EXISTS dbo.qry_tmp;
  DROP TABLE IF EXISTS dbo.qry_tmp_2;

END;
