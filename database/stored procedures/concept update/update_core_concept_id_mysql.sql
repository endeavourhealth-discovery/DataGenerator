CREATE PROCEDURE update_core_concept_id (
    IN tableName         VARCHAR(100),
    IN last_updated_date DATETIME
)

BEGIN

SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DROP TEMPORARY TABLE IF EXISTS qry_tmp;
CREATE TEMPORARY TABLE qry_tmp (
      id      INT, 
      legacy  INT,
      core    INT,
      updated DATETIME
) AS
SELECT
      id,
      legacy,
      core,
      updated
FROM concept_map
WHERE updated > last_updated_date
AND deleted = 0;

ALTER TABLE qry_tmp ADD INDEX qry_legacy_ix (legacy);
ALTER TABLE qry_tmp ADD INDEX qry_core_ix (core);

DROP TEMPORARY TABLE IF EXISTS qry_tmp_2;
SET @sql = CONCAT("
CREATE TEMPORARY TABLE qry_tmp_2 (
    row_id INT, id BIGINT, legacy INT, core INT, updated DATETIME, 
    PRIMARY KEY(row_id) 
 ) AS 
SELECT (@row_no := @row_no + 1) AS row_id, t.id, q.legacy, q.core, q.updated 
FROM ", tableName," t JOIN qry_tmp q ON q.legacy = t.non_core_concept_id JOIN (SELECT @row_no := 0) s 
WHERE (q.core <> t.core_concept_id AND t.core_concept_id IS NOT NULL ) OR t.core_concept_id IS NULL ");

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @row_id = 0;

WHILE EXISTS (SELECT row_id from qry_tmp_2 WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO

    SET @sql = CONCAT("UPDATE ", tableName, " d JOIN qry_tmp_2 q ON q.id = d.id 
    SET d.core_concept_id = q.core  
    WHERE q.row_id > @row_id AND q.row_id <= @row_id + 1000");

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @row_id = @row_id + 1000; 

END WHILE;

END


 