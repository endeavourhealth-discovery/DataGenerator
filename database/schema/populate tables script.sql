-- delete the extract data to allow the datasets to be deleted
delete from data_generator.extract where extract_id <= 5;

-- delete some dataset data
delete from data_generator.dataset where dataset_id <= 5;

-- delete some cohort data
delete from data_generator.cohort where id <= 5;


-- insert into the subscriber_file_sender table
delete from data_generator.subscriber_file_sender where subscriber_id = 1;
insert into data_generator.subscriber_file_sender
select 1, true, '{
 "subscriberFileLocationDetails": {
          "dataDir": "C:/Subscriber/Data/Sub1/",
          "stagingDir": "C:/Subscriber/Staging/Sub1/",
          "destinationDir": "/endeavour/ftp/Remote_Server/incoming/Sub1/",
          "archiveDir": "C:/Subscriber/Archive/Sub1/",
          "pgpCertFile": "C:/Subscriber/PGPCert/discovery.cer",
          "resultsSourceDir": "/endeavour/ftp/Remote_Server/result/Sub1/",
          "resultsStagingDir": "C:/Subscriber/ResultsStaging/Sub1/",
          "privateKeyFile": "C:/Subscriber/PrivateKey/sftp02endeavour.ppk"
},
 "sftpConnectionDetails": {
          "hostname": "10.0.101.239",
          "hostPublicKey": "",
          "port": "22",
          "username": "endeavour",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}';

delete from data_generator.subscriber_file_sender where subscriber_id = 2;
insert into data_generator.subscriber_file_sender
select 2, true, '{
 "subscriberFileLocationDetails": {
          "dataDir": "C:/Subscriber/Data/Sub2/",
          "stagingDir": "C:/Subscriber/Staging/Sub2/",
          "destinationDir": "/endeavour/ftp/Remote_Server/incoming/Sub2/",
          "archiveDir": "C:/Subscriber/Archive/Sub2/",
          "pgpCertFile": "C:/Subscriber/PGPCert/discovery.cer",
          "resultsSourceDir": "/endeavour/ftp/Remote_Server/result/Sub2/",
          "resultsStagingDir": "C:/Subscriber/ResultsStaging/Sub2/",
          "privateKeyFile": "C:/Subscriber/PrivateKey/sftp02endeavour.ppk"
},
 "sftpConnectionDetails": {
          "hostname": "10.0.101.239",
          "hostPublicKey": "",
          "port": "22",
          "username": "endeavour",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}';


-- insert into the dataset table
insert into data_generator.dataset
select 1, '{
 "name": "Child Imms",
 "id": "1",
 "extract": [{
  		"type": "patient",
  		"fields": [
{"header" : "patient id", "index" : "0"},
{"header" : "patient resource id", "index" : "1"},
{"header" : "nhs number", "index" : "3"},
{"header" : "date of birth", "index" : "5"},
{"header" : "gender concept id", "index": "7"},
{"header" : "ethnic code", "index" : "18"},
{"header" : "home address id", "index" : "16"},
{"header" : "address line 1", "index" : "19"},
{"header" : "address line 2", "index" : "20"},
{"header" : "address line 3", "index" : "21"},
{"header" : "address line 4", "index" : "22"},
{"header" : "postcode", "index" : "23"},
{"header" : "organisation id", "index" : "2"},
{"header" : "ods code", "index" : "27"},
{"header" : "organisation name", "index" : "28"}]
},{
  		"type": "immunisation",
  		"fields": [
{"header" : "immunisation id", "index" : "0"},
{"header" : "immunisation resource id", "index" : "1"},
{"header" : "patient resource id", "index" : "2"},
{"header" : "effective date", "index" : "4"},
{"header" : "original code", "index" : "13"},
{"header" : "original term", "index" : "14"}],
 		"codeSets": [
{"codeSetId" : 1, "extractType" : "all"}]
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
{"header" : "patient id", "index" : "0"},
{"header" : "patient resource id", "index" : "1"},
{"header" : "nhs number", "index" : "3"},
{"header" : "date of birth", "index" : "5"},
{"header" : "date of death", "index" : "6"},
{"header" : "gender concept id", "index": "7"},
{"header" : "ethnic code", "index" : "18"},
{"header" : "organisation id", "index" : "2"},
{"header" : "ods code", "index" : "27"},
{"header" : "organisation name", "index" : "28"}]
},{
  		"type": "allergy",
  		"fields": [
{"header" : "allergy id", "index" : "0"},
{"header" : "allergy resource id", "index" : "1"},
{"header" : "patient resource id", "index" : "2"},
{"header" : "concept id", "index" : "3"},
{"header" : "effective date", "index" : "4"},
{"header" : "effective practitioner id", "index" : "6"},
{"header" : "entered by practitioner id", "index" : "7"},
{"header" : "care activity id", "index" : "8"},
{"header" : "care activity heading concept id", "index" : "9"},
{"header" : "owning organisation id", "index" : "10"},
{"header" : "status concept id", "index" : "11"},
{"header" : "is confidential", "index" : "12"},
{"header" : "original code", "index" : "13"},
{"header" : "original term", "index" : "14"},
{"header" : "original code scheme", "index" : "15"},
{"header" : "original system", "index" : "16"},
{"header" : "substance concept id", "index" : "17"},
{"header" : "manifestation concept id", "index" : "18"},
{"header" : "manifestation free text id", "index" : "19"},
{"header" : "is consent", "index" : "20"}]
},{
  		"type": "medication",
  		"fields": [
{"header" : "medication id", "index" : "0"},
{"header" : "medication resource id", "index" : "1"},
{"header" : "patient resource id", "index" : "2"},
{"header" : "effective date", "index" : "4"},
{"header" : "original code", "index" : "13"},
{"header" : "original term", "index" : "14"},
{"header" : "issues authorised", "index" : "19"},
{"header" : "is active", "index" : "24"},
{"header" : "end date", "index" : "25"},
{"header" : "issues", "index" : "28"},
{"header" : "medication amount id", "index" : "18"},
{"header" : "dose", "index" : "30"},
{"header" : "quantity value", "index" : "31"},
{"header" : "quantity units", "index" : "32"}],
 		"codeSets": [
{"codeSetId": 19, "extractType": "latest_each"},
{"codeSetId": 25, "extractType": "latest_each"}]
},{
  		"type": "observation",
  		"fields": [
{"header" : "observation id", "index" : "0"},
{"header" : "observation resource id", "index" : "1"},
{"header" : "patient resource id", "index" : "2"},
{"header" : "effective date", "index" : "4"},
{"header" : "original code", "index" : "12"},
{"header" : "original term", "index" : "13"},
{"header" : "result value", "index" : "21"},
{"header" : "result value units", "index" : "22"}],
 		"codeSets": [
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
{"codeSetId": 49, "extractType": "latest_each"},
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

-- insert into the dataset table
insert into data_generator.dataset
select 3, '{
 "name": "Diabetes",
 "id": "3",
 "extract": [{
  		"type": "patient",
  		"fields": [
{"header" : "patient id", "index" : "0"},
{"header" : "patient resource id", "index" : "1"},
{"header" : "nhs number", "index" : "3"},
{"header" : "date of birth", "index" : "5"},
{"header" : "date of death", "index" : "6"},
{"header" : "gender concept id", "index": "7"},
{"header" : "ethnic code", "index" : "18"},
{"header" : "title", "index" : "11"},
{"header" : "first name", "index" : "12"},
{"header" : "middle names", "index" : "13"},
{"header" : "last name", "index" : "14"},
{"header" : "home address id", "index" : "16"},
{"header" : "address line 1", "index" : "19"},
{"header" : "address line 2", "index" : "20"},
{"header" : "address line 3", "index" : "21"},
{"header" : "address line 4", "index" : "22"},
{"header" : "postcode", "index" : "23"},
{"header" : "organisation id", "index" : "2"},
{"header" : "ods code", "index" : "27"},
{"header" : "organisation name", "index" : "28"},
{"header" : "registered date", "index" : "29"},
{"header" : "usual practitioner number", "index" : "30"}]
},{
   		"type": "observation",
  		"fields": [
{"header" : "observation id", "index" : "0"},
{"header" : "observation resource id", "index" : "1"},
{"header" : "patient resource id", "index" : "2"},
{"header" : "effective date", "index" : "4"},
{"header" : "original code", "index" : "12"},
{"header" : "original term", "index" : "13"},
{"header" : "result value", "index" : "21"},
{"header" : "result value units", "index" : "22"}],
 		"codeSets": [
{"codeSetId": 2, "extractType": "latest_each"},
{"codeSetId": 3, "extractType": "latest_each"},
{"codeSetId": 4, "extractType": "latest_each"},
{"codeSetId": 5, "extractType": "latest_each"},
{"codeSetId": 49, "extractType": "latest_each"},
{"codeSetId": 57, "extractType": "latest_each"}]
}]
}';

insert into data_generator.dataset
select 4, '{
 "name": "All Patients",
 "id": "4",
 "extract": [{
  		"type": "patient",
  		"fields": [
{"header" : "patient id", "index" : "0"},
{"header" : "patient resource id", "index" : "1"},
{"header" : "nhs number", "index" : "3"},
{"header" : "date of birth", "index" : "5"},
{"header" : "date of death", "index" : "6"},
{"header" : "gender concept id", "index": "7"},
{"header" : "ethnic code", "index" : "18"},
{"header" : "title", "index" : "11"},
{"header" : "first name", "index" : "12"},
{"header" : "middle names", "index" : "13"},
{"header" : "last name", "index" : "14"},
{"header" : "home address id", "index" : "16"},
{"header" : "address line 1", "index" : "19"},
{"header" : "address line 2", "index" : "20"},
{"header" : "address line 3", "index" : "21"},
{"header" : "address line 4", "index" : "22"},
{"header" : "postcode", "index" : "23"},
{"header" : "organisation id", "index" : "2"},
{"header" : "ods code", "index" : "27"},
{"header" : "organisation name", "index" : "28"},
{"header" : "registered date", "index" : "29"},
{"header" : "usual practitioner number", "index" : "30"}]
}]
}';

insert into data_generator.dataset
select 5, '{
 "name": "Asthma",
 "id": "3",
 "extract": [{
  		"type": "patient",
  		"fields": [
{"header" : "patient id", "index" : "0"},
{"header" : "patient resource id", "index" : "1"},
{"header" : "nhs number", "index" : "3"},
{"header" : "date of birth", "index" : "5"},
{"header" : "date of death", "index" : "6"},
{"header" : "gender concept id", "index": "7"},
{"header" : "ethnic code", "index" : "18"},
{"header" : "title", "index" : "11"},
{"header" : "first name", "index" : "12"},
{"header" : "middle names", "index" : "13"},
{"header" : "last name", "index" : "14"},
{"header" : "home address id", "index" : "16"},
{"header" : "address line 1", "index" : "19"},
{"header" : "address line 2", "index" : "20"},
{"header" : "address line 3", "index" : "21"},
{"header" : "address line 4", "index" : "22"},
{"header" : "postcode", "index" : "23"},
{"header" : "organisation id", "index" : "2"},
{"header" : "ods code", "index" : "27"},
{"header" : "organisation name", "index" : "28"},
{"header" : "registered date", "index" : "29"},
{"header" : "usual practitioner number", "index" : "30"}]
},{
  		"type": "medication",
  		"fields": [
{"header" : "medication id", "index" : "0"},
{"header" : "medication resource id", "index" : "1"},
{"header" : "patient resource id", "index" : "2"},
{"header" : "effective date", "index" : "4"},
{"header" : "original code", "index" : "13"},
{"header" : "original term", "index" : "14"},
{"header" : "issues authorised", "index" : "19"},
{"header" : "is active", "index" : "24"},
{"header" : "end date", "index" : "25"},
{"header" : "issues", "index" : "28"},
{"header" : "medication amount id", "index" : "18"},
{"header" : "dose", "index" : "30"},
{"header" : "quantity value", "index" : "31"},
{"header" : "quantity units", "index" : "32"}],
 		"codeSets": [
{"codeSetId": 69, "extractType": "latest_each"}]
},{
   		"type": "observation",
  		"fields": [
{"header" : "observation id", "index" : "0"},
{"header" : "observation resource id", "index" : "1"},
{"header" : "patient resource id", "index" : "2"},
{"header" : "effective date", "index" : "4"},
{"header" : "original code", "index" : "12"},
{"header" : "original term", "index" : "13"},
{"header" : "result value", "index" : "21"},
{"header" : "result value units", "index" : "22"}],
 		"codeSets": [
{"codeSetId": 68, "extractType": "latest_each"}]
}]
}';

-- create some extract data for extract 1
insert into data_generator.extract
select 1, 'Subscriber A Child Imms', 1, 1, 1, '{
 "name": "Data Generator Extract Definition 1",
 "id": "1",
 "projectId": "DISCOCH",
 "fileLocationDetails": {
          "source": "/datagenerator/SubscriberA/ChildImms/Source/",
          "destination": "/ftp/SubscriberA/ChildImms/",
          "housekeep": "/datagenerator/SubscriberA/ChildImms/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "n3sftp01.discoverydataservice.net",
          "hostPublicKey": "",
          "port": "990",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0,'',true;

-- create some extract data for extract 2
insert into data_generator.extract
select 2, 'Subscriber A Health Check', 2, 1, 2, '{
 "name": "Data Generator Extract Definition 2",
 "id": "2",
 "projectId": "DISCOHC",
 "fileLocationDetails": {
          "source": "/datagenerator/SubscriberA/HealthCheck/Source/",
          "destination": "/ftp/SubscriberA/HealthCheck/",
          "housekeep": "/datagenerator/SubscriberA/HealthCheck/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "n3sftp01.discoverydataservice.net",
          "hostPublicKey": "",
          "port": "990",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0,'',false;

-- create some extract data for extract 3
insert into data_generator.extract
select 3, 'Subscriber A Diabetes', 3, 1, 3, '{
 "name": "Data Generator Extract Definition 3",
 "id": "3",
 "projectId": "DISCODE",
 "fileLocationDetails": {
          "source": "/datagenerator/SubscriberA/Diabetes/Source/",
          "destination": "/ftp/SubscriberA/Diabetes/",
          "housekeep": "/datagenerator/SubscriberA/Diabetes/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "n3sftp01.discoverydataservice.net",
          "hostPublicKey": "",
          "port": "990",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0,'',true;

-- create some extract data for extract 4
insert into data_generator.extract
select 4, 'Subscriber A All Patients', 4, 1, 4, '{
 "name": "Data Generator Extract Definition 4",
 "id": "4",
 "projectId": "DISCODE",
 "fileLocationDetails": {
          "source": "/datagenerator/SubscriberA/Asthma/Source/",
          "destination": "/ftp/SubscriberA/Asthma/",
          "housekeep": "/datagenerator/SubscriberA/Asthma/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "n3sftp01.discoverydataservice.net",
          "hostPublicKey": "",
          "port": "990",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0,'',true;

-- create some extract data for extract 5
insert into data_generator.extract
select 5, 'Subscriber A Asthma', 5, 1, 5, '{
 "name": "Data Generator Extract Definition 5",
 "id": "5",
 "projectId": "DISCODE",
 "fileLocationDetails": {
          "source": "/datagenerator/SubscriberA/AllPatients/Source/",
          "destination": "/ftp/SubscriberA/AllPatients/",
          "housekeep": "/datagenerator/SubscriberA/AllPatients/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "n3sftp01.discoverydataservice.net",
          "hostPublicKey": "",
          "port": "990",
          "username": "endeavour",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0,'',true;

insert into data_generator.cohort (id, title, xml_content)
values (1, 'Child Immunisation Patients Under 20', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Child Immunisation Patients Under 20</name>
    <description>Under 20s</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
    <organisations>1</organisations>
    <query>
        <startingRules>
            <ruleId>1</ruleId>
        </startingRules>
        <rule>
            <description>Age</description>
            <id>1</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>0</code>
                            <term>Age</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Patient</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo>20</valueTo>
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
values (2, 'Health Check Patients', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Health Check Patients</name>
    <description>Health Check Patients</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
    <organisations>1</organisations>
    <query>
        <startingRules>
            <ruleId>1</ruleId>
        </startingRules>
        <rule>
            <description>Age</description>
            <id>1</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>0</code>
                            <term>Age</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Patient</baseType>
                            <present>1</present>
                            <valueFrom>40</valueFrom>
                            <valueTo>75</valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
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
            <description>Inappropriate For NHS Health Check Exclusion</description>
            <id>2</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>8</code>
                            <term>Inappropriate for NHS Health Check codes</term>
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
                <action>noAction</action>
            </onPass>
            <onFail>
                <action>gotoRules</action>
				<ruleId>3</ruleId>
            </onFail>
            <layout>
                <x>566</x>
                <y>7</y>
            </layout>
        </rule>
		<rule>
            <description>Pre-existing Conditions Exclusion</description>
            <id>3</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>14</code>
                            <term>Coronary heart disease codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>11</code>
                            <term>Chronic kidney disease codes 3-5</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>23</code>
                            <term>Codes for diabetes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>17</code>
                            <term>Hypertension diagnosis codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>9</code>
                            <term>Atrial fibrillation codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>22</code>
                            <term>TIA codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>15</code>
                            <term>Familial and Non-Familial Hypercholesterolemia diagnostic codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>16</code>
                            <term>Heart failure codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>20</code>
                            <term>Peripheral arterial disease</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>21</code>
                            <term>Stroke diagnosis codes</term>
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
                <action>noAction</action>
            </onPass>
            <onFail>
                <action>gotoRules</action>
				<ruleId>4</ruleId>
            </onFail>
            <layout>
                <x>566</x>
                <y>7</y>
            </layout>
        </rule>
		<rule>
            <description>Current Statin Prescription</description>
            <id>4</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>25</code>
                            <term>Statin Codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Medication Statement</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
                    </codeSet>
                    <negate>false</negate>
                </filter>
                <filter>
                   <field>MEDICATION_STATUS</field>
                   <valueTo>
                       <constant>1</constant>
                       <absoluteUnit>numeric</absoluteUnit>
                       <testField>is_active</testField>
                       <operator>lessThanOrEqualTo</operator>
                   </valueTo>
                   <negate>false</negate>
                </filter>
            </test>
            <onPass>
                <action>noAction</action>
            </onPass>
            <onFail>
                <action>include</action>
            </onFail>
            <layout>
                <x>566</x>
                <y>7</y>
            </layout>
        </rule>
    </query>
</libraryItem>');

insert into data_generator.cohort (id, title, xml_content)
values (3, 'Diabetes Patients Aged 12+', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Diabetes Patients Aged 12+</name>
    <description>Diabetes Patients Aged 12+</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
    <organisations>1</organisations>
    <query>
        <startingRules>
            <ruleId>1</ruleId>
        </startingRules>
        <rule>
            <description>Age</description>
            <id>1</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>0</code>
                            <term>Age</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Patient</baseType>
                            <present>1</present>
                            <valueFrom>12</valueFrom>
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
            <description>Diabetes</description>
            <id>2</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>3</code>
                            <term>Diabetes - National List</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>4</code>
                            <term>Diabetes Resolved</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Observation</baseType>
                            <present>1</present>
                            <valueFrom></valueFrom>
                            <valueTo></valueTo>
                            <units></units>
                            <includeChildren>false</includeChildren>
                        </codeSetValue>
						<codeSetValue>
                            <code>5</code>
                            <term>Diabetes - Local QOF List</term>
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
                <action>include</action>
            </onPass>
            <onFail>
                <action>noAction</action>
            </onFail>
            <layout>
                <x>566</x>
                <y>7</y>
            </layout>
        </rule>
    </query>
</libraryItem>');

insert into data_generator.cohort (id, title, xml_content)
values (4, 'All Patients', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>PCR All Patients</name>
    <description>All Patients</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
    <organisations>1</organisations>
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
values (5, 'Asthma Patients', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Asthma Patients</name>
    <description>Asthma Patients</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
    <organisations>1</organisations>
    <query>
        <startingRules>
            <ruleId>1</ruleId>
        </startingRules>
        <rule>
            <description>Asthma Diagnosis</description>
            <id>1</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
                        <codeSetValue>
                            <code>68</code>
                            <term>Asthma diagnosis codes</term>
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
				<ruleId>2</ruleId>
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
            <description>Asthma Medication</description>
            <id>2</id>
            <type>1</type>
            <test>
                <filter>
                    <field>CONCEPT</field>
                    <codeSet>
                        <codingSystem>Endeavour</codingSystem>
						<codeSetValue>
                            <code>69</code>
                            <term>Asthma medication codes</term>
                            <dataType>11</dataType>
                            <parentType></parentType>
                            <baseType>Medication Statement</baseType>
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
                <action>include</action>
            </onPass>
            <onFail>
                <action>noAction</action>
            </onFail>
            <layout>
                <x>566</x>
                <y>7</y>
            </layout>
        </rule>
    </query>
</libraryItem>');
