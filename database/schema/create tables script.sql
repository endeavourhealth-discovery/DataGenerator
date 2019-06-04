drop database if exists data_generator;

create database data_generator;

drop table if exists data_generator.subscriber_file_sender;
drop table if exists data_generator.subscriber_zip_file_uuids;
drop table if exists data_generator.dataset;
drop table if exists data_generator.extract;
drop table if exists data_generator.cohort_results;
drop table if exists data_generator.file_transactions;
drop table if exists data_generator.cohort;
drop table if exists data_generator.exported_ids;

create table data_generator.subscriber_file_sender (
	subscriber_id int not null comment 'The id of the subscriber file send.',
    definition varchar(5000) not null comment 'The json definition of the subscriber file send.' ,
    
    primary key (subscriber_id)
);

create table data_generator.subscriber_zip_file_uuids (
	subscriber_id int not null comment 'The id of the subscriber file send.',
    queued_message_uuid varchar(36) not null comment 'The uuid identifying the zip file data in the message_body field 
													  in the audit.queued_message table.',
    filing_order int not null comment 'Incrementing field to retain the order of applying the zip files.',                                                  
    -- queued_message_timestamp datetime not null comment 'The timestamp field of the entry in the audit.queued_message table.',
    -- queued_message_type_id int(11) not null comment 'The type_id field of the entry in the audit.queued_message table.',
    file_sent boolean not null default false comment 'Whether or not the file has been sent to the SFTP.',
    
    primary key (queued_message_uuid)
);
    
create table data_generator.dataset (
	dataset_id int not null comment 'The id of the dataset',
    definition mediumtext not null comment 'The json definition of the dataset',
    
    constraint data_generator_dataset_dataset_id_pk primary key (dataset_id)
);

create table data_generator.extract (
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
    foreign key data_generator_extract_dataset_id_fk (dataset_id) references data_generator.dataset(dataset_id)    
);

create table data_generator.cohort_results (
	extract_id int not null comment 'The extract that this patient is related to',
    patient_id bigint not null comment 'The patient id to be extracted',
    organisation_id bigint not null comment 'The organisation id the patient is related to',
    bulked boolean not null default 0 comment 'Whether the patient has been bulked',
    
    constraint data_generator_cohort_results_patient_id_pk primary key (extract_id, patient_id, organisation_id),
    foreign key data_generator_cohort_results_extract_id_fk (extract_id) references data_generator.extract(extract_id) on delete cascade
);

create table data_generator.file_transactions (
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

CREATE TABLE data_generator.cohort (
  id int NOT NULL,
  title varchar(255) NOT NULL,
  xml_content longtext NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE data_generator.exported_ids
(
  extract_id int NOT NULL,
  item_id bigint NOT NULL,
  table_id int NOT NULL,
  CONSTRAINT pk_exported_ids PRIMARY KEY (extract_id, item_id, table_id),
  FOREIGN KEY fk_exported_ids_extract_id (extract_id) REFERENCES data_generator.extract (extract_id)
);
