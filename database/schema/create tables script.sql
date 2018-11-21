drop database if exists data_generator;

create database data_generator;

drop table if exists data_generator.dataset;
drop table if exists data_generator.extract;
drop table if exists data_generator.cohort_results;
drop table if exists data_generator.file_transactions;

create table data_generator.dataset (
	dataset_id int not null comment 'The id of the dataset',
    definition varchar(5000) not null comment 'The json definition of the extract',
    
    constraint data_generator_dataset_dataset_id_pk primary key (dataset_id)
);

create table data_generator.extract (
	extract_id int not null comment 'The extract that this patient is related to',
    extract_name varchar(50) not null comment 'The name of the extract',
    cohort_id int not null comment 'The cohort to be used for this extract',
    code_set_id int not null comment 'The code set to be used for this extract',
    dataset_id int not null comment 'The dataset to be used for this extract',
    definition varchar(5000) not null comment 'The json definition of the extract',
    transaction_id bigint not null comment 'The latest transaction Id that was extracted',
    
    constraint data_generator_extract_id_pk primary key (extract_id),
    foreign key data_generator_extract_dataset_id_fk (dataset_id) references data_generator.dataset(dataset_id) on delete cascade    
);

create table data_generator.cohort_results (
	extract_id int not null comment 'The extract that this patient is related to',
    patient_id bigint not null comment 'The patient id to be extracted',
    organisation_id bigint not null comment 'The organisation Id the patient is related to',
    bulked boolean not null default 0 comment 'Whether the patient has been bulked',
    
    constraint data_generator_cohort_results_patient_id_pk primary key (patient_id),
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
