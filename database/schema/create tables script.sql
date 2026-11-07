drop database if exists data_generator;

create database data_generator;

use data_generator;

drop table if exists dataset;
drop table if exists extract;
drop table if exists cohort_results;
drop table if exists file_transactions;
drop table if exists cohort;
drop table if exists exported_ids;
drop table if exists last_queued_message_sent;

CREATE TABLE subscriber_file_sender (
  subscriber_id int not null comment 'The id of the subscriber file send',
  subscriber_live boolean not null comment 'Whether or not the subscriber is live',
  definition varchar(5000) not null comment 'The json definition of the subscriber file send',
  PRIMARY KEY (subscriber_id)
);

CREATE TABLE subscriber_zip_file_uuids (
  subscriber_id int not null comment 'The id of the subscriber file send',
  batch_uuid varchar(36) not null comment 'The uuid identifying the batch of which the zip file data is a part',
  queued_message_uuid varchar(36) not null comment 'The uuid identifying the zip file data in the message_body field in the audit.queued_message table',
  queued_message_body mediumtext comment 'The zip file (of the set of csv files) data that has come from that table',
  -- queued_message_timestamp datetime not null comment 'The timestamp field of the entry in the audit.queued_message table',
  -- queued_message_type_id int(11) not null comment 'The type_id field of the entry in the audit.queued_message table',
  filing_order bigint not null comment 'Incrementing field to retain the order of applying the zip files',
  file_sent datetime comment 'When the file was sent to the SFTP',
  file_filing_attempted datetime comment 'When the filing was attempted at the receiving end',
  file_filing_success boolean comment 'Whether the filing was a success at the receiving end',
  filing_failure_message text comment 'The error message generated at the receiving end',
  service_id char(36) COMMENT 'ID of the publisher service',
  system_id char(36) COMMENT 'system ID of the publisher service',
  exchange_id char(36) COMMENT 'links to audit.exchange table',
  dt_inserted datetime COMMENT 'timestamp of being inserted into this table',
  PRIMARY KEY (queued_message_uuid)
);

CREATE INDEX ix_filing_order ON subscriber_zip_file_uuids(filing_order);

CREATE INDEX ix_subscriber_filing_order_sent ON subscriber_zip_file_uuids(subscriber_id, filing_order, file_sent);

CREATE INDEX ix_subscriber_sent_inserted ON subscriber_zip_file_uuids (subscriber_id, file_sent, dt_inserted);

CREATE INDEX ix_subscriber_success_sent ON subscriber_zip_file_uuids (subscriber_id, file_filing_success, file_sent);

CREATE INDEX ix_subscriber_sent ON subscriber_zip_file_uuids (subscriber_id, file_sent);

CREATE INDEX ix_subscriber ON subscriber_zip_file_uuids (subscriber_id);

CREATE INDEX ix_subscriber_sent_filing_order ON subscriber_zip_file_uuids (subscriber_id, file_sent, filing_order);

CREATE INDEX ix_subscriber_unsuccess_attempted ON data_generator.subscriber_zip_file_uuids (subscriber_id, file_filing_success, file_filing_attempted);

CREATE INDEX ix_subscriber_service_unsuccess ON data_generator.subscriber_zip_file_uuids (subscriber_id, service_id, file_filing_success);

create table subscriber_zip_file_uuids_archive (
    subscriber_id int not null comment 'The id of the subscriber file send.',
    batch_uuid varchar(36) not null comment 'The uuid identifying the batch of which the zip file data is a part.',
    queued_message_uuid varchar(36) not null comment 'The uuid identifying the zip file data in the message_body field
													  in the audit.queued_message table.',
    -- queued_message_body mediumtext comment 'The zip file (of the set of csv files) data that has come from that table.',
    -- queued_message_timestamp datetime not null comment 'The timestamp field of the entry in the audit.queued_message table.',
    -- queued_message_type_id int(11) not null comment 'The type_id field of the entry in the audit.queued_message table.',
    filing_order bigint not null comment 'Incrementing field to retain the order of applying the zip files.',
    file_sent datetime comment 'When the file was sent to the SFTP.',
    file_filing_attempted datetime comment 'When the filing was attempted at the receiving end.',
    file_filing_success boolean comment 'Whether the filing was a success at the receiving end.',
    filing_failure_message text comment 'The error message generated at the receiving end.',

    primary key (queued_message_uuid)
);

create INDEX subscriber_zip_file_uuids_archive_filing_order_asc
    ON subscriber_zip_file_uuids_archive
        (filing_order ASC);

create INDEX subscriber_zip_file_uuids_archive_filing_order_desc
    ON subscriber_zip_file_uuids_archive
        (filing_order DESC);


create table dataset (
	dataset_id int not null comment 'The id of the dataset',
    definition mediumtext not null comment 'The json definition of the dataset',
    
    constraint data_generator_dataset_dataset_id_pk primary key (dataset_id)
);

create table extract (
	extract_id int not null comment 'The extract that this patient is related to',
    extract_name varchar(50) not null comment 'The name of the extract',
    cohort_id int not null comment 'The cohort to be used for this extract',
    code_set_id int not null comment 'The code set to be used for this extract',
    dataset_id int not null comment 'The dataset to be used for this extract',
    definition varchar(5000) not null comment 'The json definition of the extract',
    transaction_id bigint not null comment 'The latest transaction id that was extracted',
    cron varchar(30) null comment 'The cron timing of the extract',
    clear_cohort_every_run boolean comment 'Whether to clear down the cohort every time the extract is run or keep existing matching patients',
    
    constraint data_generator_extract_id_pk primary key (extract_id),
    foreign key data_generator_extract_dataset_id_fk (dataset_id) references dataset(dataset_id)    
);

create table cohort_results (
	extract_id int not null comment 'The extract that this patient is related to',
    patient_id bigint not null comment 'The patient id to be extracted',
    organisation_id bigint not null comment 'The organisation id the patient is related to',
    bulked boolean not null default 0 comment 'Whether the patient has been bulked',
    
    constraint data_generator_cohort_results_patient_id_pk primary key (extract_id, patient_id, organisation_id),
    foreign key data_generator_cohort_results_extract_id_fk (extract_id) references extract(extract_id) on delete cascade
);

create table file_transactions (
	extract_id int not null comment 'The extract that this patient is related to',
	filename varchar(50) not null comment 'Unique Filename',
	extract_date datetime default null comment 'Date and time when the extract file was created',
	zip_date datetime default null comment 'Date and time when the extract file was zipped',
	encrypt_date datetime default null comment 'Date and time when the extract file was encrypted',
	sftp_date datetime default null comment 'Date and time when the encrypted extract file was sent via sftp',
	housekeeping_date datetime default null comment 'Date and time when the encrypted extract file was kept for storage',
  
	primary key (extract_id,filename),
	unique key filename_unique (filename)
);

CREATE TABLE cohort (
  id int NOT NULL,
  title varchar(255) NOT NULL,
  xml_content longtext NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE exported_ids
(
  extract_id int NOT NULL,
  item_id bigint NOT NULL,
  table_id int NOT NULL,
  CONSTRAINT pk_exported_ids PRIMARY KEY (extract_id, item_id, table_id),
  FOREIGN KEY fk_exported_ids_extract_id (extract_id) REFERENCES extract (extract_id)
);

CREATE TABLE last_queued_message_sent (
   subscriber_id int(11) NOT NULL COMMENT 'remote subscriber ID',
   queued_message_id char(36) NOT NULL COMMENT 'id of the message sent',
   service_id char(36) NOT NULL COMMENT 'service UUID this is for',
   system_id char(36) NOT NULL COMMENT 'system UUID this is for',
   exchange_id char(36) DEFAULT NULL COMMENT 'exchange UUID of the last exchange sent',
   extract_date datetime NOT NULL COMMENT 'datetime the extract was generated by publisher',
   extract_cutoff datetime NOT NULL COMMENT 'datetime of the last data in the extract',
   written_date datetime DEFAULT NULL COMMENT 'datetime the queued message was written to the subscriber DB',
   PRIMARY KEY (subscriber_id, queued_message_id)
)
ROW_FORMAT=COMPRESSED
KEY_BLOCK_SIZE=8;

-- index required by DDS-UI to find ones that have actually been written to the subscriber DB
CREATE INDEX ix_subscriber_written ON last_queued_message_sent (subscriber_id, written_date);