-- delete the extract data to allow the datasets to be deleted
delete from data_generator.extract where extract_id <= 4;

-- delete some dataset data
delete from data_generator.dataset where dataset_id = 1;
delete from data_generator.dataset where dataset_id = 2;

-- insert into the dataset table
insert into data_generator.dataset
select 1, '{
 "name": "Child Imms",
 "id": "1",
 "extract": [{
  		"type": "patient",
  		"fields": [
{"header" : "id", "index" : "0"},
{"header" : "organisation id", "index" : "1"},
{"header" : "nhs number", "index" : "2"},
{"header" : "date of birth", "index" : "4"},
{"header" : "gender concept id", "index": "6"},
{"header" : "usual practitioner id", "index": "7"},
{"header" : "care provider id", "index" : "8"},
{"header" : "entered by practitioner id", "index" : "9"},
{"header" : "title", "index" : "10"},
{"header" : "first name", "index" : "11"},
{"header" : "middle names", "index" : "12"},
{"header" : "last name", "index" : "13"},
{"header" : "previous last name", "index" : "14"},
{"header" : "home address id", "index" : "15"},
{"header" : "address line 1", "index" : "17"},
{"header" : "address line 2", "index" : "18"},
{"header" : "address line 3", "index" : "19"},
{"header" : "address line 4", "index" : "20"},
{"header" : "postcode", "index" : "21"},
{"header" : "uprn", "index" : "22"}]
},{
  		"type": "immunisation",
  		"fields": [
{"header" : "id", "index" : "0"},
{"header" : "patient id", "index" : "1"},
{"header" : "concept id", "index" : "2"},
{"header" : "effective date", "index" : "3"},
{"header" : "effective practitioner id", "index" : "5"},
{"header" : "care activity id", "index" : "7"},
{"header" : "care activity heading concept id", "index" : "8"},
{"header" : "owning organisation id", "index" : "9"},
{"header" : "status concept id", "index" : "10"},
{"header" : "is confidential", "index" : "11"},
{"header" : "original code", "index" : "12"},
{"header" : "original term", "index" : "13"},
{"header" : "original code scheme", "index" : "14"},
{"header" : "original system", "index" : "15"},
{"header" : "dose", "index" : "16"},
{"header" : "body location concept id", "index" : "17"},
{"header" : "method concept id", "index" : "18"},
{"header" : "batch number", "index" : "19"},
{"header" : "expiry date", "index" : "20"},
{"header" : "manufacturer", "index" : "21"},
{"header" : "dose ordinal", "index" : "22"},
{"header" : "doses required", "index" : "23"},
{"header" : "is consent", "index" : "24"}],
 		"codeSets": [
{"codeSetId" : 1, "extractType" : "latest_each"}]
}]
}';

-- insert into the dataset table
insert into data_generator.dataset
select 2, '{
 "name": "Health Check",
 "id": "2",
 "extract": [{
  		"type": "patient",
  		"fields": [
{"header" : "id", "index" : "0"},
{"header" : "organisation id", "index" : "1"},
{"header" : "nhs number", "index" : "2"},
{"header" : "date of birth", "index" : "4"},
{"header" : "gender concept id", "index": "6"},
{"header" : "usual practitioner id", "index": "7"},
{"header" : "care provider id", "index" : "8"},
{"header" : "entered by practitioner id", "index" : "9"},
{"header" : "title", "index" : "10"},
{"header" : "first name", "index" : "11"},
{"header" : "middle names", "index" : "12"},
{"header" : "last name", "index" : "13"},
{"header" : "previous last name", "index" : "14"},
{"header" : "home address id", "index" : "15"},
{"header" : "address line 1", "index" : "17"},
{"header" : "address line 2", "index" : "18"},
{"header" : "address line 3", "index" : "19"},
{"header" : "address line 4", "index" : "20"},
{"header" : "postcode", "index" : "21"},
{"header" : "uprn", "index" : "22"}]
},{
  		"type": "allergy",
  		"fields": [
{"header" : "id", "index" : "0"},
{"header" : "patient id", "index" : "1"},
{"header" : "concept id", "index" : "2"},
{"header" : "effective date", "index" : "3"},
{"header" : "effective practitioner id", "index" : "5"},
{"header" : "entered by practitioner id", "index" : "6"},
{"header" : "care activity id", "index" : "7"},
{"header" : "care activity heading concept id", "index" : "8"},
{"header" : "owning organisation id", "index" : "9"},
{"header" : "status concept id", "index" : "10"},
{"header" : "is confidential", "index" : "11"},
{"header" : "original code", "index" : "12"},
{"header" : "original term", "index" : "13"},
{"header" : "original code scheme", "index" : "14"},
{"header" : "original system", "index" : "15"},
{"header" : "substance concept id", "index" : "16"},
{"header" : "manifestation concept id", "index" : "17"},
{"header" : "manifestation free text id", "index" : "18"},
{"header" : "is consent", "index" : "19"}],
 		"codeSets": [
{"codeSetId": 1, "extractType": "all"},
{"codeSetId": 2, "extractType": "latest"},
{"codeSetId": 3, "extractType": "earliest"},
{"codeSetId": 4, "extractType": "latest_each"},
{"codeSetId": 5, "extractType": "earliest_each"}]
},{
  		"type": "immunisation",
  		"fields": [
{"header" : "id", "index" : "0"},
{"header" : "patient id", "index" : "1"},
{"header" : "concept id", "index" : "2"},
{"header" : "effective date", "index" : "3"},
{"header" : "effective practitioner id", "index" : "5"},
{"header" : "care activity id", "index" : "7"},
{"header" : "care activity heading concept id", "index" : "8"},
{"header" : "owning organisation id", "index" : "9"},
{"header" : "status concept id", "index" : "10"},
{"header" : "is confidential", "index" : "11"},
{"header" : "original code", "index" : "12"},
{"header" : "original term", "index" : "13"},
{"header" : "original code scheme", "index" : "14"},
{"header" : "original system", "index" : "15"},
{"header" : "dose", "index" : "16"},
{"header" : "body location concept id", "index" : "17"},
{"header" : "method concept id", "index" : "18"},
{"header" : "batch number", "index" : "19"},
{"header" : "expiry date", "index" : "20"},
{"header" : "manufacturer", "index" : "21"},
{"header" : "dose ordinal", "index" : "22"},
{"header" : "doses required", "index" : "23"},
{"header" : "is consent", "index" : "24"}],
 		"codeSets": [
{"codeSetId" : 1, "extractType" : "latest_each"}]
},{
  		"type": "medication",
  		"fields": [
{"header" : "id", "index" : "0"},
{"header" : "patient id", "index" : "1"},
{"header" : "drug concept id", "index" : "2"},
{"header" : "effective date", "index" : "3"},
{"header" : "effective practitioner id", "index" : "5"},
{"header" : "entered by practitioner id", "index" : "6"},
{"header" : "care activity id", "index" : "7"},
{"header" : "care activity heading concept id", "index" : "8"},
{"header" : "owning organisation id", "index" : "9"},
{"header" : "status concept id", "index" : "10"},
{"header" : "is confidential", "index" : "11"},
{"header" : "original code", "index" : "12"},
{"header" : "original term", "index" : "13"},
{"header" : "original code scheme", "index" : "14"},
{"header" : "original system", "index" : "15"},
{"header" : "type concept id", "index" : "16"},
{"header" : "medication amount id", "index" : "17"},
{"header" : "issues authorised", "index" : "18"},
{"header" : "review date", "index" : "19"},
{"header" : "course length per issue days", "index" : "20"},
{"header" : "patient instructions free text id", "index" : "21"},
{"header" : "pharmacy instructions free text id", "index" : "22"},
{"header" : "is active", "index" : "23"},
{"header" : "end date", "index" : "24"},
{"header" : "end reason concept id", "index" : "25"},
{"header" : "end reason free text id", "index" : "26"},
{"header" : "issues", "index" : "27"},
{"header" : "is consent", "index" : "28"}],
 		"codeSets": [
{"codeSetId": 19, "extractType": "latest_each"},
{"codeSetId": 25, "extractType": "latest_each"}]
},{
  		"type": "observation",
  		"fields": [
{"header" : "id", "index" : "0"},
{"header" : "patient id", "index" : "1"},
{"header" : "concept id", "index" : "2"},
{"header" : "effective date", "index" : "3"},
{"header" : "effective practitioner id", "index" : "5"},
{"header" : "entered by practitioner id", "index" : "6"},
{"header" : "care activity id", "index" : "7"},
{"header" : "care activity heading concept id", "index" : "8"},
{"header" : "owning organisation id", "index" : "9"},
{"header" : "is confidential", "index" : "10"},
{"header" : "original code", "index" : "11"},
{"header" : "original term", "index" : "12"},
{"header" : "original code scheme", "index" : "13"},
{"header" : "original system", "index" : "14"},
{"header" : "episodicity concept id", "index" : "15"},
{"header" : "free text id", "index" : "16"},
{"header" : "data entry prompt id", "index" : "17"},
{"header" : "significance concept id", "index" : "18"},
{"header" : "is consent", "index" : "19"}],
 		"codeSets": [
{"codeSetId": 2, "extractType": "latest"},
{"codeSetId": 3, "extractType": "latest"},
{"codeSetId": 4, "extractType": "latest"},
{"codeSetId": 5, "extractType": "latest"},
{"codeSetId": 6, "extractType": "latest"},
{"codeSetId": 7, "extractType": "latest"},
{"codeSetId": 8, "extractType": "latest"},
{"codeSetId": 9, "extractType": "latest"},
{"codeSetId": 10, "extractType": "latest"},
{"codeSetId": 11, "extractType": "latest"},
{"codeSetId": 12, "extractType": "latest"},
{"codeSetId": 13, "extractType": "latest"},
{"codeSetId": 14, "extractType": "latest"},
{"codeSetId": 15, "extractType": "latest"},
{"codeSetId": 16, "extractType": "latest"},
{"codeSetId": 17, "extractType": "latest"},
{"codeSetId": 18, "extractType": "latest"},
{"codeSetId": 20, "extractType": "latest"},
{"codeSetId": 21, "extractType": "latest"},
{"codeSetId": 22, "extractType": "latest"},
{"codeSetId": 23, "extractType": "latest"},
{"codeSetId": 24, "extractType": "latest"},
{"codeSetId": 26, "extractType": "latest"},
{"codeSetId": 27, "extractType": "latest"},
{"codeSetId": 28, "extractType": "latest"},
{"codeSetId": 29, "extractType": "latest"},
{"codeSetId": 30, "extractType": "latest"},
{"codeSetId": 31, "extractType": "latest"},
{"codeSetId": 32, "extractType": "latest"},
{"codeSetId": 33, "extractType": "latest"},
{"codeSetId": 34, "extractType": "latest"},
{"codeSetId": 35, "extractType": "latest"},
{"codeSetId": 36, "extractType": "latest"},
{"codeSetId": 37, "extractType": "latest"},
{"codeSetId": 38, "extractType": "latest"},
{"codeSetId": 39, "extractType": "latest"},
{"codeSetId": 40, "extractType": "latest"},
{"codeSetId": 41, "extractType": "latest"},
{"codeSetId": 42, "extractType": "latest"},
{"codeSetId": 43, "extractType": "latest"},
{"codeSetId": 44, "extractType": "latest"},
{"codeSetId": 45, "extractType": "latest"},
{"codeSetId": 46, "extractType": "latest"},
{"codeSetId": 47, "extractType": "latest"},
{"codeSetId": 48, "extractType": "latest"},
{"codeSetId": 49, "extractType": "latest"},
{"codeSetId": 50, "extractType": "latest"},
{"codeSetId": 51, "extractType": "latest"},
{"codeSetId": 52, "extractType": "latest"},
{"codeSetId": 53, "extractType": "latest"},
{"codeSetId": 54, "extractType": "latest"},
{"codeSetId": 55, "extractType": "latest"},
{"codeSetId": 56, "extractType": "latest"},
{"codeSetId": 57, "extractType": "latest"},
{"codeSetId": 58, "extractType": "latest"},
{"codeSetId": 59, "extractType": "latest"},
{"codeSetId": 60, "extractType": "latest"},
{"codeSetId": 61, "extractType": "latest"},
{"codeSetId": 62, "extractType": "latest"},
{"codeSetId": 63, "extractType": "latest"},
{"codeSetId": 64, "extractType": "latest"},
{"codeSetId": 65, "extractType": "latest"},
{"codeSetId": 66, "extractType": "latest"},
{"codeSetId": 67, "extractType": "latest"}]
}]
}';

-- delete some extract data
delete from data_generator.extract where extract_id <= 4;

-- create some extract data for extract 1
insert into data_generator.extract
select 1, 'Subscriber A Child Imms', 1, 1, 1, '{
 "name": "Data Generator Extract Definition 1",
 "id": "1",
 "projectId": "R1D68",
 "fileLocationDetails": {
          "source": "C:/DataGenerator/SubscriberA/ChildImms/Source/",
          "destination": "/endeavour/ftp/SubscriberA/ChildImms/",
          "housekeep": "C:/DataGenerator/SubscriberA/ChildImms/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "10.0.101.239",
          "hostPublicKey": "",
          "port": "22",
          "username": "endeavour",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": "PuTTY-User-Key"
}
}',0;

-- create some extract data for extract 2
insert into data_generator.extract
select 2, 'Subscriber B Health Check', 1, 1, 2, '{
 "name": "Data Generator Extract Definition 2",
 "id": "2",
 "projectId": "8JM56",
 "fileLocationDetails": {
          "source": "C:/DataGenerator/SubscriberA/HealthCheck/Source/",
          "destination": "/endeavour/ftp/SubscriberA/HealthCheck/",
          "housekeep": "C:/DataGenerator/SubscriberA/HealthCheck/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "10.0.101.239",
          "hostPublicKey": "",
          "port": "22",
          "username": "endeavour",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": "PuTTY-User-Key"
}
}',0;

-- create some extract data for extract 3
insert into data_generator.extract
select 3, 'Subscriber B Child Imms', 1, 1, 1, '{
 "name": "Data Generator Extract Definition 3",
 "id": "3",
 "projectId": "RJD69",
 "fileLocationDetails": {
          "source": "C:/DataGenerator/SubscriberB/ChildImms/Source/",
          "destination": "/endeavour/ftp/SubscriberB/ChildImms/",
          "housekeep": "C:/DataGenerator/SubscriberB/ChildImms/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "10.0.101.239",
          "hostPublicKey": "",
          "port": "22",
          "username": "endeavour",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": "PuTTY-User-Key"
}
}',0;

-- create some extract data for extract 4
insert into data_generator.extract
select 4, 'Subscriber B Health Check', 1, 1, 2, '{
 "name": "Data Generator Extract Definition 4",
 "id": "4",
 "projectId": "RJD69",
 "fileLocationDetails": {
          "source": "C:/DataGenerator/SubscriberB/HealthCheck/Source/",
          "destination": "/endeavour/ftp/SubscriberB/HealthCheck/",
          "housekeep": "C:/DataGenerator/SubscriberB/HealthCheck/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "10.0.101.239",
          "hostPublicKey": "",
          "port": "22",
          "username": "endeavour",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": "PuTTY-User-Key"
}
}',0;

-- populate the cohort results table with data from the pcr tables
insert into data_generator.cohort_results
select 1, id, organisation_id, 0 from pcr.patient;

insert into data_generator.cohort_results
select 2, id, organisation_id, 0 from pcr.patient;

-- look at the state of the file-transactions queue for the Java class processes 
-- ZipCSVFiles, EncryptFiles, TransferEncryptedFilesToSftp & HousekeepFiles    
select * from data_generator.file_transactions;

-- delete some file_transactions data
-- delete from data_generator.file_transactions where extract_id = 1;
-- delete from data_generator.file_transactions where extract_id = 2;

-- create some file_transactions data 
-- insert into data_generator.file_transactions
-- select 1, '1_20181130', now(), null, null, null, null;
-- insert into data_generator.file_transactions
-- select 2, '2_20181130', now(), null, null, null, null;
-- insert into data_generator.file_transactions
-- select 3, '3_20181130', now(), null, null, null, null;
-- insert into data_generator.file_transactions
-- select 4, '4_20181130', now(), null, null, null, null;

-- update data_generator.file_transactions
-- set encrypt_date = now() where extract_id = 1 and filename = '1_20181126.z01';
-- update data_generator.file_transactions
-- set encrypt_date = now() where extract_id = 1 and filename = '2_20181126.zip';

delete from data_generator.cohort;

insert into data_generator.cohort (id, title, xml_content)
values (1, 'All Patients', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>PCR All Patients</name>
    <description>PCR All Patients</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
    <query>
        <startingRules>
            <ruleId>1</ruleId>
        </startingRules>
        <rule>
            <description>Date of Birth</description>
            <id>1</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>8833263929279</code>
                            <term>Date of Birth</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Patient</baseType>
                            <present>1</present>
                            <valueFrom>1800-01-01</valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>true</includeChildren>
                        </codeSetValue>
                    </codeSet>
                    <negate>false</negate>
                </filter>
            </test>
            <onPass>
                <action>include</action>
            </onPass>
            <onFail>
                <action>noAction</action>
            </onFail>
            <layout>
                <x>547</x>
                <y>202</y>
            </layout>
        </rule>
    </query>
</libraryItem>');

insert into data_generator.cohort (id, title, xml_content)
values (2, 'Example Cohort with Multiple Rules', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Example Cohort with Multiple Rules</name>
    <description>Example Cohort with Multiple Rules</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
    <query>
        <startingRules>
            <ruleId>1</ruleId>
        </startingRules>
        <rule>
            <description>Date of Birth</description>
            <id>1</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>8833263929279</code>
                            <term>Date of Birth</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Patient</baseType>
                            <present>1</present>
                            <valueFrom>1967-01-01</valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>true</includeChildren>
                        </codeSetValue>
                    </codeSet>
                    <negate>false</negate>
                </filter>
            </test>
            <onPass>
                <action>gotoRules</action>
                <ruleId>2</ruleId>
            </onPass>
            <onFail>
                <action>noAction</action>
            </onFail>
            <layout>
                <x>194</x>
                <y>5</y>
            </layout>
        </rule>
        <rule>
            <description>Asthma</description>
            <id>2</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>H33..</code>
                            <term>Asthma</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>true</includeChildren>
                        </codeSetValue>
                        <codeSetValue>
                            <code>C10..</code>
                            <term>Diabetes mellitus (disorder)</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
                    </codeSet>
                    <negate>false</negate>
                </filter>
            </test>
            <onPass>
                <action>gotoRules</action>
                <ruleId>3</ruleId>
            </onPass>
            <onFail>
                <action>noAction</action>
            </onFail>
            <layout>
                <x>566</x>
                <y>7</y>
            </layout>
        </rule>
        <rule>
            <description>Co-codamol 30mg/500mg capsules</description>
            <id>3</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>XYZ..</code>
                            <term>Co-codamol 30mg/500mg capsules</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Medication Statement</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>true</includeChildren>
                        </codeSetValue>
                    </codeSet>
                    <negate>false</negate>
                </filter>
            </test>
            <onPass>
                <action>include</action>
            </onPass>
            <onFail>
                <action>noAction</action>
            </onFail>
            <layout>
                <x>547</x>
                <y>202</y>
            </layout>
        </rule>
    </query>
</libraryItem>');