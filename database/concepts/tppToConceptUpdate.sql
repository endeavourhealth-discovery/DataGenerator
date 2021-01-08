USE information_model;  
DROP PROCEDURE IF EXISTS tppToConceptUpdate;
DELIMITER //
CREATE PROCEDURE tppToConceptUpdate(p_tablename VARCHAR(64))
BEGIN
  -- create a temporary table for tpp local codes
  DROP TABLE IF EXISTS tpp_local_codes_tmp;
  SET @sql = CONCAT("CREATE TEMPORARY TABLE tpp_local_codes_tmp (
         local_term VARCHAR(255) DEFAULT NULL, 
         local_code VARCHAR(250) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL, 
         local_code_id VARCHAR(250) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL, 
         scheme BIGINT(20) DEFAULT NULL, 
         mapped_snomed_concept_id BIGINT(20), 
         snomed_id VARCHAR(150)
        ) AS 
  SELECT DISTINCT 
         SUBSTRING(ctv3_term, 1, 255) local_term, 
         ctv3_code AS local_code,  
         CONCAT('TPPLOC_', ctv3_code) AS local_code_id, 
         1440863 AS scheme,  
         snomed_concept_id AS mapped_snomed_concept_id, 
         CONCAT('SN_', snomed_concept_id) AS snomed_id 
   FROM ", p_tablename);
   PREPARE stmt FROM @sql;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;
  ALTER TABLE tpp_local_codes_tmp ADD INDEX loc_code_idx(local_code_id);
  -- create a temporary table to hold local codes that exist but have a different term
  DROP TABLE IF EXISTS tpp_local_exist_tmp;
  CREATE TABLE tpp_local_exist_tmp (
           row_id INT(11), 
           local_term VARCHAR(255) DEFAULT NULL,
           local_code VARCHAR(50) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
           local_code_id VARCHAR(50) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
           scheme BIGINT(20) DEFAULT NULL,
           mapped_snomed_concept_id BIGINT(20),
           snomed_id VARCHAR(50),
           PRIMARY KEY (row_id)
   ) AS 
   SELECT (@row_no := @row_no + 1) AS row_id, 
           a.local_term,
           a.local_code,
           a.local_code_id,
           a.scheme,
           a.mapped_snomed_concept_id,
           a.snomed_id
   FROM tpp_local_codes_tmp a JOIN concept cpt ON cpt.id = a.local_code_id
   JOIN (SELECT @row_no := 0) t
   WHERE cpt.name <> a.local_term;   
   ALTER TABLE tpp_local_exist_tmp ADD INDEX loc_code_idx(local_code_id);
   SET @row_id = 0;
   -- update the existing codes with the new terms
   WHILE EXISTS (SELECT row_id from tpp_local_exist_tmp WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO
         UPDATE concept cpt JOIN tpp_local_exist_tmp q ON cpt.id = q.local_code_id 
         SET cpt.name = q.local_term, cpt.description = q.local_term, cpt.updated = now()
         WHERE q.row_id > @row_id AND q.row_id <= @row_id + 1000;     
         SET @row_id = @row_id + 1000;
   END WHILE;
   -- drop this temporary table
   -- DROP TABLE IF EXISTS tpp_local_exist_tmp;
  -- create a temporary table to hold local codes that are brand new
  DROP TABLE IF EXISTS tpp_local_new_tmp;
  CREATE TABLE tpp_local_new_tmp (
           row_id INT(11), 
           local_term VARCHAR(255) DEFAULT NULL,
           local_code VARCHAR(50) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
           local_code_id VARCHAR(50) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
           scheme BIGINT(20) DEFAULT NULL,
           mapped_snomed_concept_id BIGINT(20),
           snomed_id VARCHAR(50),
           PRIMARY KEY (row_id)
   ) AS 
   SELECT (@row_no := @row_no + 1) AS row_id, 
           a.local_term,
           a.local_code,
           a.local_code_id,
           a.scheme,
           a.mapped_snomed_concept_id,
           a.snomed_id
   FROM tpp_local_codes_tmp a JOIN (SELECT @row_no := 0) t
   WHERE NOT EXISTS ( SELECT 1 FROM concept cpt WHERE cpt.id = a.local_code_id);
   ALTER TABLE tpp_local_new_tmp ADD INDEX loc_code_idx(local_code_id);
   -- insert new codes into concept
   SET @row_id = 0;
   WHILE EXISTS (SELECT row_id from tpp_local_new_tmp WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO
         INSERT INTO concept (document, id, draft, name, description, scheme, code, use_count, updated)
         SELECT 1, q.local_code_id, 0, q.local_term, q.local_term, q.scheme, q.local_code, 0, now()
         FROM  tpp_local_new_tmp q WHERE q.row_id > @row_id AND q.row_id <= @row_id + 1000;  
         SET @row_id = @row_id + 1000;
   END WHILE;
   -- drop this temporary table
   -- DROP TABLE IF EXISTS tpp_local_new_tmp;
   -- create a temporary table to hold all new concepts with a matching snomed concept
   DROP TABLE IF EXISTS tpp_local_cpo_new_tmp;
   CREATE TABLE tpp_local_cpo_new_tmp AS
   SELECT c1.dbid AS local_dbid, 
          c2.dbid AS snomed_dbid
   FROM tpp_local_codes_tmp l JOIN concept c1 ON c1.id = l.local_code_id AND c1.scheme = 1440863
   JOIN concept c2 ON c2.id = l.snomed_id AND c2.scheme = 71;
   -- create a temporary table to hold new/changed concept property object records
   DROP TABLE IF EXISTS tpp_local_cpo_update_tmp;
   CREATE TABLE tpp_local_cpo_update_tmp (
          row_id INT(11), 
          local_dbid INT(11),
          snomed_dbid INT(11),
          PRIMARY KEY (row_id)
   ) AS 
   SELECT (@row_no := @row_no + 1) AS row_id, 
         l.local_dbid, 
         l.snomed_dbid
   FROM tpp_local_cpo_new_tmp l
   LEFT JOIN concept_property_object cpo ON cpo.dbid = l.local_dbid AND cpo.property = 50
   JOIN (SELECT @row_no := 0) t 
   WHERE cpo.value IS NULL OR cpo.value != l.snomed_dbid;
   SET @row_id = 0;
   -- insert new records into concept property object
   WHILE EXISTS (SELECT row_id from tpp_local_cpo_update_tmp WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO
         INSERT INTO concept_property_object (dbid, `group`, property, `value`, updated)
         SELECT q.local_dbid, 0, 50, q.snomed_dbid, now()
         FROM  tpp_local_cpo_update_tmp q WHERE q.row_id > @row_id AND q.row_id <= @row_id + 1000;  
         SET @row_id = @row_id + 1000;
   END WHILE;
   DROP TABLE IF EXISTS tpp_local_cpo_delete_tmp;
   CREATE TABLE tpp_local_cpo_delete_tmp (
          row_id INT(11), 
          local_dbid INT(11),
          snomed_dbid INT(11),
          PRIMARY KEY (row_id)
   ) AS 
   SELECT (@row_no := @row_no + 1) AS row_id, 
         l.local_dbid, 
         l.snomed_dbid
   FROM tpp_local_cpo_update_tmp l JOIN concept_property_object cpo ON cpo.dbid = l.local_dbid AND cpo.property = 50
   JOIN (SELECT @row_no := 0) t 
   WHERE cpo.value != l.snomed_dbid;
   SET @row_id = 0;
   -- delete old concept property object records
   WHILE EXISTS (SELECT row_id from tpp_local_cpo_delete_tmp WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO
       DELETE cpo FROM concept_property_object cpo JOIN tpp_local_cpo_delete_tmp q ON cpo.dbid = q.local_dbid AND cpo.property = 50 
       WHERE q.snomed_dbid != cpo.value
       AND q.row_id > @row_id AND q.row_id <= @row_id + 1000;  
       SET @row_id = @row_id + 1000;
   END WHILE;
   DROP TABLE IF EXISTS tpp_local_cm_insert_tmp;
   CREATE TABLE tpp_local_cm_insert_tmp (
          row_id INT(11), 
          local_dbid INT(11),
          snomed_dbid INT(11),
          PRIMARY KEY (row_id)
   ) AS 
   SELECT (@row_no := @row_no + 1) AS row_id, 
         l.local_dbid, 
         l.snomed_dbid
   FROM tpp_local_cpo_new_tmp l
   LEFT JOIN concept_map cm ON cm.legacy = l.local_dbid
   JOIN (SELECT @row_no := 0) t 
   WHERE cm.deleted = 0
   AND (cm.core IS NULL OR cm.core != l.snomed_dbid);
   SET @row_id = 0;
   -- insert new records into concept property object
   WHILE EXISTS (SELECT row_id from tpp_local_cm_insert_tmp WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO
         INSERT INTO concept_map (legacy, core, updated, deleted )
         SELECT q.local_dbid, snomed_dbid, now(), 0 
         FROM  tpp_local_cm_insert_tmp q WHERE q.row_id > @row_id AND q.row_id <= @row_id + 1000;  
         SET @row_id = @row_id + 1000;
   END WHILE;
   DROP TABLE IF EXISTS tpp_local_cm_update_tmp;
   CREATE TABLE tpp_local_cm_update_tmp (
          row_id INT(11), 
          local_dbid INT(11),
          snomed_dbid INT(11),
          PRIMARY KEY (row_id)
   ) AS 
   SELECT (@row_no := @row_no + 1) AS row_id, 
         l.local_dbid, 
         l.snomed_dbid
   FROM tpp_local_cm_insert_tmp l JOIN concept_map cm ON cm.legacy = l.local_dbid
   JOIN (SELECT @row_no := 0) t 
   WHERE cm.deleted = 0
   AND cm.core != l.snomed_dbid;
   SET @row_id = 0;
   -- update old concept map records if found
   WHILE EXISTS (SELECT row_id from tpp_local_cm_update_tmp WHERE row_id > @row_id AND row_id <= @row_id + 1000) DO
       UPDATE concept_map cm JOIN tpp_local_cm_update_tmp q ON cm.legacy = q.local_dbid 
       SET deleted = 1, updated = now()
       WHERE q.snomed_dbid != cm.core AND cm.deleted = 0
       AND q.row_id > @row_id AND q.row_id <= @row_id + 1000;
       SET @row_id = @row_id + 1000;
   END WHILE;
      -- create a temporary table to hold all local codes that are without a mapped snomed on the concept table
   DROP TABLE IF EXISTS emis_local_without_snomed_tmp;
   CREATE TABLE emis_local_without_snomed_tmp AS
   SELECT c1.dbid AS local_dbid, 
          l.local_term,
          l.local_code,
          l.scheme,
          l.mapped_snomed_concept_id,
          l.snomed_id
   FROM emis_local_codes_tmp l JOIN concept c1 ON c1.id = l.local_code_id AND c1.scheme = 1440863
   LEFT JOIN concept c2 ON c2.id = l.snomed_id AND c2.scheme = 71
   WHERE c2.dbid IS NULL;
END //
DELIMITER ;