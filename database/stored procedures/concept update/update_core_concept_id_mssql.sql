CREATE PROCEDURE update_core_concept_id(
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
   id         BIGINT,
   legacy     BIGINT, 
   core       BIGINT, 
   updated    DATETIME
  );

  CREATE INDEX legacy_idx ON dbo.qry_tmp(legacy);

  WITH qry AS (
    SELECT cm.id, cm.legacy, cm.core, cm.updated 
    FROM  dbo.concept_map cm 
    WHERE cm.updated > @last_updated_date
    AND cm.deleted = 0
  )
  INSERT INTO dbo.qry_tmp
  SELECT * FROM qry;

  DROP TABLE IF EXISTS dbo.qry_tmp_2;

  CREATE TABLE dbo.qry_tmp_2( 
   id         BIGINT,
   legacy     BIGINT, 
   core       BIGINT,
   row_id     BIGINT NOT NULL PRIMARY KEY
  );

  SET @sql1 = N'WITH qry2 AS ( '+
    'SELECT t.id, q.legacy, q.core, ROW_NUMBER() OVER(ORDER BY q.id) AS row_id '+
    'FROM ' + @table + ' t INNER JOIN dbo.qry_tmp q ON q.legacy = t.non_core_concept_id '+
    'WHERE (q.core <> t.core_concept_id AND t.core_concept_id IS NOT NULL) OR t.core_concept_id IS NULL '+
  ') '+
  'INSERT INTO dbo.qry_tmp_2 '+
  'SELECT * FROM qry2; ';

  EXEC sp_executesql @sql1; 

  SELECT @cnt = COUNT(*) FROM dbo.qry_tmp_2;
 
  WHILE (@i < @cnt)

  BEGIN

  SET @sql = N'UPDATE t '+ 
      'SET t.core_concept_id = q.core '+
      'FROM ' + @table + ' t INNER JOIN dbo.qry_tmp_2 q ON q.id = t.id '+
      'WHERE q.row_id > '+ CONVERT(VARCHAR(20), @i) + ' AND q.row_id <= ' +  CONVERT(VARCHAR(20), @i)  + ' + 1000;'; 
  
  EXEC sp_executesql @sql;
 
  SELECT @i = @i + 1000; 

  END;

  DROP TABLE IF EXISTS dbo.qry_tmp;
  DROP TABLE IF EXISTS dbo.qry_tmp_2;

END;
