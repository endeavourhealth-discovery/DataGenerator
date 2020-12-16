CREATE PROCEDURE update_bnf_reference (
    IN tableName         VARCHAR(100),
    IN last_updated_date DATETIME
)

BEGIN

SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DROP TEMPORARY TABLE IF EXISTS qry_tmp;
CREATE TEMPORARY TABLE qry_tmp (
      snomed_code varchar(20), 
      bnf_chapter_code  varchar(6),
      dt_last_updated DATETIME
) AS
SELECT
      snomed_code,
      substring(bnf_chapter_code,1,6) bnf_chapter_code,
      dt_last_updated
FROM snomed_to_bnf_chapter_lookup
WHERE dt_last_updated > last_updated_date;

ALTER TABLE qry_tmp ADD INDEX qry_snomed_code_ix (snomed_code);
ALTER TABLE qry_tmp ADD INDEX qry_bnf_chapter_code_ix (bnf_chapter_code);

DROP TEMPORARY TABLE IF EXISTS qry_tmp_2;
SET @sql = CONCAT("
CREATE TEMPORARY TABLE qry_tmp_2 (row_id BIGINT, id BIGINT, snomed_code varchar(20), bnf_chapter_code varchar(6), PRIMARY KEY(row_id)) AS 
select (@row_no := @row_no + 1) AS row_id, t.id id, c.code snomed_code, q.bnf_chapter_code  
from concept c JOIN ",tableName," t ON t.non_core_concept_id = c.dbid 
JOIN qry_tmp q ON q.snomed_code = c.code JOIN (SELECT @row_no := 0) s  
WHERE ((t.bnf_reference <> q.bnf_chapter_code AND t.bnf_reference IS NOT NULL) OR t.bnf_reference IS NULL) and c.scheme = 71
");

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @row_id = 0;

WHILE EXISTS (SELECT row_id from qry_tmp_2 WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO

    SET @sql = CONCAT("UPDATE ", tableName, " d JOIN qry_tmp_2 q ON q.id = d.id 
    SET d.bnf_reference = q.bnf_chapter_code  
    WHERE q.row_id > @row_id AND q.row_id <= @row_id + 1000");

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @row_id = @row_id + 1000; 

END WHILE;

END
