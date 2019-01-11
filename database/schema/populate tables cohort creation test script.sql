-- All Patients
-- Cohort Count Test

insert into data_generator.dataset
select 1, '{
 "name": "All Patients",
 "id": "1",
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

insert into data_generator.extract
select 1, 'Cohort Count Test All Patients', 1, 1, 1, '{
 "name": "Data Generator Extract Definition 1",
 "id": "1",
 "projectId": "DISCODE",
 "fileLocationDetails": {
          "source": "/datagenerator/CohortTest/AllPatients/Source/",
          "destination": "",
          "housekeep": "/datagenerator/CohortTest/AllPatients/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "",
          "hostPublicKey": "",
          "port": "",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0;

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


-- Diabetes Aged 12+ Patient
-- Cohort Count Test

insert into data_generator.dataset
select 2, '{
 "name": "Diabetes",
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
{"codeSetId": 5, "extractType": "latest_each"}]
}]
}';

insert into data_generator.extract
select 2, 'Cohort Count Test Diabetes', 2, 1, 2, '{
 "name": "Data Generator Extract Definition 2",
 "id": "2",
 "projectId": "DISCODE",
 "fileLocationDetails": {
          "source": "/datagenerator/CohortTest/Diabetes/Source/",
          "destination": "",
          "housekeep": "/datagenerator/CohortTest/Diabetes/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "",
          "hostPublicKey": "",
          "port": "",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0;

insert into data_generator.cohort (id, title, xml_content)
values (2, 'Diabetes Patients Aged 12+', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Diabetes Aged 12+</name>
    <description>Diabetes Aged 12+</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
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
                            <term>Diabetes - Local OOF List</term>
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


-- Asthma Patients
-- Cohort Count Test

insert into data_generator.dataset
select 3, '{
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

insert into data_generator.extract
select 3, 'Cohort Count Test Asthma', 3, 1, 3, '{
 "name": "Data Generator Extract Definition 3",
 "id": "3",
 "projectId": "DISCODE",
 "fileLocationDetails": {
          "source": "/datagenerator/CohortTest/Asthma/Source/",
          "destination": "",
          "housekeep": "/datagenerator/CohortTest/Asthma/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "",
          "hostPublicKey": "",
          "port": "",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0;

insert into data_generator.cohort (id, title, xml_content)
values (3, 'Asthma Patients', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Asthma Patients</name>
    <description>Asthma Patients</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
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


-- Health Check
-- Cohort Count Test

insert into data_generator.dataset
select 4, '{
 "name": "Health Check",
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

insert into data_generator.extract
select 4, 'Cohort Count Test Health Check', 4, 1, 4, '{
 "name": "Data Generator Extract Definition 4",
 "id": "4",
 "projectId": "DISCOHC",
 "fileLocationDetails": {
          "source": "/datagenerator/CohortTest/HealthCheck/Source/",
          "destination": "",
          "housekeep": "/datagenerator/CohortTest/HealthCheck/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "",
          "hostPublicKey": "",
          "port": "",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0;

insert into data_generator.cohort (id, title, xml_content)
values (4, 'Health Check Patients', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Health Check Population</name>
    <description>Health Check Population</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
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
                            <valueTo>74</valueTo>
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


-- Child Immunisations
-- Cohort Count Test

insert into data_generator.dataset
select 5, '{
 "name": "Child Imms",
 "id": "5",
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
{"codeSetId" : 1, "extractType" : "latest_each"}]
}]
}';

insert into data_generator.extract
select 5, 'Cohort Count Test Child Imms', 5, 1, 5, '{
 "name": "Data Generator Extract Definition 5",
 "id": "5",
 "projectId": "DISCOCH",
 "fileLocationDetails": {
          "source": "/datagenerator/CohortTest/ChildImms/Source/",
          "destination": "",
          "housekeep": "/datagenerator/CohortTest/ChildImms/Housekeep/"
},
 "sftpConnectionDetails": {
          "hostname": "",
          "hostPublicKey": "",
          "port": "",
          "username": "",
          "clientPrivateKeyPassword": "",
          "clientPrivateKey": ""
}
}',0;

insert into data_generator.cohort (id, title, xml_content)
values (5, 'Child Immunisation Patients Under 20', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<libraryItem>
    <uuid>c6b126ff-f457-4e08-9dbb-0f033b8bf4ab</uuid>
    <name>Under 20s</name>
    <description>Under 20s</description>
    <folderUuid>7f58e4d1-2f85-446b-b433-cdf3a6e21078</folderUuid>
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