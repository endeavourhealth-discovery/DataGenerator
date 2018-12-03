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
{"header" : "title", "index" : "10"},
{"header" : "first name", "index" : "11"},
{"header" : "middle names", "index" : "12"},
{"header" : "last name", "index" : "13"},
{"header" : "previous last name", "index" : "14"},
{"header" : "home address id", "index" : "15"},
{"header" : "ethnic code", "index" : "17"}]
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
{"header" : "title", "index" : "10"},
{"header" : "first name", "index" : "11"},
{"header" : "middle names", "index" : "12"},
{"header" : "last name", "index" : "13"},
{"header" : "previous last name", "index" : "14"},
{"header" : "home address id", "index" : "15"},
{"header" : "ethnic code", "index" : "17"}]
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
{"header" : "concept id", "index" : "2"},
{"header" : "effective date", "index" : "3"},
{"header" : "is consent", "index" : "19"}],
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
{"codeSetId": 2, "extractType": "all"},
{"codeSetId": 3, "extractType": "all"},
{"codeSetId": 4, "extractType": "all"},
{"codeSetId": 5, "extractType": "all"},
{"codeSetId": 6, "extractType": "all"},
{"codeSetId": 7, "extractType": "all"},
{"codeSetId": 8, "extractType": "all"},
{"codeSetId": 9, "extractType": "all"},
{"codeSetId": 10, "extractType": "all"},
{"codeSetId": 11, "extractType": "all"},
{"codeSetId": 12, "extractType": "all"},
{"codeSetId": 13, "extractType": "all"},
{"codeSetId": 14, "extractType": "all"},
{"codeSetId": 15, "extractType": "all"},
{"codeSetId": 16, "extractType": "all"},
{"codeSetId": 17, "extractType": "all"},
{"codeSetId": 18, "extractType": "all"},
{"codeSetId": 20, "extractType": "all"},
{"codeSetId": 21, "extractType": "all"},
{"codeSetId": 22, "extractType": "all"},
{"codeSetId": 23, "extractType": "all"},
{"codeSetId": 24, "extractType": "all"},
{"codeSetId": 26, "extractType": "all"},
{"codeSetId": 27, "extractType": "all"},
{"codeSetId": 28, "extractType": "all"},
{"codeSetId": 29, "extractType": "all"},
{"codeSetId": 30, "extractType": "all"},
{"codeSetId": 31, "extractType": "all"},
{"codeSetId": 32, "extractType": "all"},
{"codeSetId": 33, "extractType": "all"},
{"codeSetId": 34, "extractType": "all"},
{"codeSetId": 35, "extractType": "all"},
{"codeSetId": 36, "extractType": "all"},
{"codeSetId": 37, "extractType": "all"},
{"codeSetId": 38, "extractType": "all"},
{"codeSetId": 39, "extractType": "all"},
{"codeSetId": 40, "extractType": "all"},
{"codeSetId": 41, "extractType": "all"},
{"codeSetId": 42, "extractType": "all"},
{"codeSetId": 43, "extractType": "all"},
{"codeSetId": 44, "extractType": "all"},
{"codeSetId": 45, "extractType": "all"},
{"codeSetId": 46, "extractType": "all"},
{"codeSetId": 47, "extractType": "all"},
{"codeSetId": 48, "extractType": "all"},
{"codeSetId": 49, "extractType": "all"},
{"codeSetId": 50, "extractType": "all"},
{"codeSetId": 51, "extractType": "all"},
{"codeSetId": 52, "extractType": "all"},
{"codeSetId": 53, "extractType": "all"},
{"codeSetId": 54, "extractType": "all"},
{"codeSetId": 55, "extractType": "all"},
{"codeSetId": 56, "extractType": "all"},
{"codeSetId": 57, "extractType": "all"},
{"codeSetId": 58, "extractType": "all"},
{"codeSetId": 59, "extractType": "all"},
{"codeSetId": 60, "extractType": "all"},
{"codeSetId": 61, "extractType": "all"},
{"codeSetId": 62, "extractType": "all"},
{"codeSetId": 63, "extractType": "all"},
{"codeSetId": 64, "extractType": "all"},
{"codeSetId": 65, "extractType": "all"},
{"codeSetId": 66, "extractType": "all"},
{"codeSetId": 67, "extractType": "all"}]
}]
}';

-- delete some extract data
delete from data_generator.extract where extract_id <= 4;

-- create some extract data for extract 1
insert into data_generator.extract
select 1, 'Subscriber A Child Imms', 1, 1, 1, '{
"name": "Data Generator Extract Definition 1",
 "id": "1",
 "projectId": "19b3bce7-60f6-4e2d-878b-fd9a5e9de08b",
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
          "clientPrivateKey": "PuTTY-User-Key-File-2: ssh-rsa\\r\\nEncryption: none\\r\\nComment: imported-openssh-key\\r\\nPublic-Lines: 6\\r\\nAAAAB3NzaC1yc2EAAAADAQABAAABAQDNb7bfn3hEjjUtnkHCgIeVzUJKRXidXoYP\\r\\na0pzIVEv9Dgo6zN4ajUE2mCCgFi2Y16yJVupRIkFkZ8d31VItkMbrZIyV0wCekDs\\r\\n0I6gxh4V5XJrsiWTHfk4fAajb2AWcjt3Id9Hray8zPoiwE0RxSNcAGHNpR9OO82S\\r\\nndotGj2bOzBXN+16lEWF/bi2ZQ77dYiYKKuzvyyD8MMXJ2E5BTewli4SW8KRxQIl\\r\\nMt1tRfRexUfjDIIn6cyOMs9WCCsr5GC2hQuyyAGNQw2pAxvUvlOYl+WP1h8IPcrS\\r\\nR7lGtmXWfl4hc8zfmR1/jafIkk5SK/W9KpYBHLLQA5EQFZezVQeL\\r\\nPrivate-Lines: 14\\r\\nAAABAFH0rE8ADnnDBcICLZfLsMt5TGXW2yxkxjSmh9fwRbRMyI7CbhEuxaH/AJtv\\r\\nWnTApcmKD8wyVDuNgZ3oN9y/IXyMPROqMd+XKAmRliTbhKsVkxUVx9muDnuTNw7C\\r\\nYuHxhnmbYLj2tz/Gwk9Uyio5rEaKvHnO0vNh9jv0j1KI8mTXevl5Eun9JFpf6bgl\\r\\nRhoobNhBtJSw4vmz352FFO2vTtaqti/idU6AVeB2XWhYDFntaPUfrFmoPMI8VUte\\r\\nhBdPIP1ty/RZQBjaccdugNJmGR08D68LObfOvpr2FH9xRKVh4KIzmaU84n1n011A\\r\\nWd3Vda3m5O6J2DsvLZWsVsGLkkkAAACBAP/aS7s8xPg6GAK7vf9c0f/Z5jYBmoWQ\\r\\nXVLdTbUvuuElgmCvpy6/tPt+fFgFqkOW64fl3RDMM6R1afSoXGH9wtCkG+zTeJam\\r\\n1HtSGQ205qTG2l5TZzIsDqgX75d0+ObIpoHorUmpRBIKz0i5VQrw0EqoTxo18P+h\\r\\nCYsSr0ccBDvtAAAAgQDNjf0kQi0fBr8uIW/dF1CAdnFpbFWsAgecqnxEHHC5wGCM\\r\\nw+tZdpzxNa/Qr1stUdDZvs6XCGs8yB70tQ5Dxac0ZeK6VM6JcF60AQomPe7GsBXH\\r\\nwGUWSa2v9szkTb4fQcrCTYL7U6SVixq8vAiVYfjExV0smuuOybzjIiXpXqYSVwAA\\r\\nAIEAiU0ecoZ+2eN6Lf9d46jxKbYmj6J675UFIxIeYaPIz4Ev1aznXwsHPg9oog3X\\r\\nlGkIJavmZAgabuiK8yjVwUtCvs1dC0KvRIDhEkVEG6y5c/ExLTf9/uWetlhVdFZQ\\r\\nK13Fln7c/PPILty6YJ7etxW7c2+QhID27IeV5GbwvEjxcpk=\\r\\nPrivate-MAC: a58be360183ea42a152010bb51b38f4277f6871e"
}
}',0;

-- create some extract data for extract 2
insert into data_generator.extract
select 2, 'Subscriber B Health Check', 1, 1, 2, '{
"name": "Data Generator Extract Definition 2",
 "id": "2",
 "projectId": "19b3bce7-60f6-4e2d-878b-fd9a5e9de08b",
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
          "clientPrivateKey": "PuTTY-User-Key-File-2: ssh-rsa\\r\\nEncryption: none\\r\\nComment: imported-openssh-key\\r\\nPublic-Lines: 6\\r\\nAAAAB3NzaC1yc2EAAAADAQABAAABAQDNb7bfn3hEjjUtnkHCgIeVzUJKRXidXoYP\\r\\na0pzIVEv9Dgo6zN4ajUE2mCCgFi2Y16yJVupRIkFkZ8d31VItkMbrZIyV0wCekDs\\r\\n0I6gxh4V5XJrsiWTHfk4fAajb2AWcjt3Id9Hray8zPoiwE0RxSNcAGHNpR9OO82S\\r\\nndotGj2bOzBXN+16lEWF/bi2ZQ77dYiYKKuzvyyD8MMXJ2E5BTewli4SW8KRxQIl\\r\\nMt1tRfRexUfjDIIn6cyOMs9WCCsr5GC2hQuyyAGNQw2pAxvUvlOYl+WP1h8IPcrS\\r\\nR7lGtmXWfl4hc8zfmR1/jafIkk5SK/W9KpYBHLLQA5EQFZezVQeL\\r\\nPrivate-Lines: 14\\r\\nAAABAFH0rE8ADnnDBcICLZfLsMt5TGXW2yxkxjSmh9fwRbRMyI7CbhEuxaH/AJtv\\r\\nWnTApcmKD8wyVDuNgZ3oN9y/IXyMPROqMd+XKAmRliTbhKsVkxUVx9muDnuTNw7C\\r\\nYuHxhnmbYLj2tz/Gwk9Uyio5rEaKvHnO0vNh9jv0j1KI8mTXevl5Eun9JFpf6bgl\\r\\nRhoobNhBtJSw4vmz352FFO2vTtaqti/idU6AVeB2XWhYDFntaPUfrFmoPMI8VUte\\r\\nhBdPIP1ty/RZQBjaccdugNJmGR08D68LObfOvpr2FH9xRKVh4KIzmaU84n1n011A\\r\\nWd3Vda3m5O6J2DsvLZWsVsGLkkkAAACBAP/aS7s8xPg6GAK7vf9c0f/Z5jYBmoWQ\\r\\nXVLdTbUvuuElgmCvpy6/tPt+fFgFqkOW64fl3RDMM6R1afSoXGH9wtCkG+zTeJam\\r\\n1HtSGQ205qTG2l5TZzIsDqgX75d0+ObIpoHorUmpRBIKz0i5VQrw0EqoTxo18P+h\\r\\nCYsSr0ccBDvtAAAAgQDNjf0kQi0fBr8uIW/dF1CAdnFpbFWsAgecqnxEHHC5wGCM\\r\\nw+tZdpzxNa/Qr1stUdDZvs6XCGs8yB70tQ5Dxac0ZeK6VM6JcF60AQomPe7GsBXH\\r\\nwGUWSa2v9szkTb4fQcrCTYL7U6SVixq8vAiVYfjExV0smuuOybzjIiXpXqYSVwAA\\r\\nAIEAiU0ecoZ+2eN6Lf9d46jxKbYmj6J675UFIxIeYaPIz4Ev1aznXwsHPg9oog3X\\r\\nlGkIJavmZAgabuiK8yjVwUtCvs1dC0KvRIDhEkVEG6y5c/ExLTf9/uWetlhVdFZQ\\r\\nK13Fln7c/PPILty6YJ7etxW7c2+QhID27IeV5GbwvEjxcpk=\\r\\nPrivate-MAC: a58be360183ea42a152010bb51b38f4277f6871e"
}
}',0;

-- create some extract data for extract 3
insert into data_generator.extract
select 3, 'Subscriber B Child Imms', 1, 1, 1, '{
"name": "Data Generator Extract Definition 3",
 "id": "3",
 "projectId": "19b3bce7-60f6-4e2d-878b-fd9a5e9de08b",
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
          "clientPrivateKey": "PuTTY-User-Key-File-2: ssh-rsa\\r\\nEncryption: none\\r\\nComment: imported-openssh-key\\r\\nPublic-Lines: 6\\r\\nAAAAB3NzaC1yc2EAAAADAQABAAABAQDNb7bfn3hEjjUtnkHCgIeVzUJKRXidXoYP\\r\\na0pzIVEv9Dgo6zN4ajUE2mCCgFi2Y16yJVupRIkFkZ8d31VItkMbrZIyV0wCekDs\\r\\n0I6gxh4V5XJrsiWTHfk4fAajb2AWcjt3Id9Hray8zPoiwE0RxSNcAGHNpR9OO82S\\r\\nndotGj2bOzBXN+16lEWF/bi2ZQ77dYiYKKuzvyyD8MMXJ2E5BTewli4SW8KRxQIl\\r\\nMt1tRfRexUfjDIIn6cyOMs9WCCsr5GC2hQuyyAGNQw2pAxvUvlOYl+WP1h8IPcrS\\r\\nR7lGtmXWfl4hc8zfmR1/jafIkk5SK/W9KpYBHLLQA5EQFZezVQeL\\r\\nPrivate-Lines: 14\\r\\nAAABAFH0rE8ADnnDBcICLZfLsMt5TGXW2yxkxjSmh9fwRbRMyI7CbhEuxaH/AJtv\\r\\nWnTApcmKD8wyVDuNgZ3oN9y/IXyMPROqMd+XKAmRliTbhKsVkxUVx9muDnuTNw7C\\r\\nYuHxhnmbYLj2tz/Gwk9Uyio5rEaKvHnO0vNh9jv0j1KI8mTXevl5Eun9JFpf6bgl\\r\\nRhoobNhBtJSw4vmz352FFO2vTtaqti/idU6AVeB2XWhYDFntaPUfrFmoPMI8VUte\\r\\nhBdPIP1ty/RZQBjaccdugNJmGR08D68LObfOvpr2FH9xRKVh4KIzmaU84n1n011A\\r\\nWd3Vda3m5O6J2DsvLZWsVsGLkkkAAACBAP/aS7s8xPg6GAK7vf9c0f/Z5jYBmoWQ\\r\\nXVLdTbUvuuElgmCvpy6/tPt+fFgFqkOW64fl3RDMM6R1afSoXGH9wtCkG+zTeJam\\r\\n1HtSGQ205qTG2l5TZzIsDqgX75d0+ObIpoHorUmpRBIKz0i5VQrw0EqoTxo18P+h\\r\\nCYsSr0ccBDvtAAAAgQDNjf0kQi0fBr8uIW/dF1CAdnFpbFWsAgecqnxEHHC5wGCM\\r\\nw+tZdpzxNa/Qr1stUdDZvs6XCGs8yB70tQ5Dxac0ZeK6VM6JcF60AQomPe7GsBXH\\r\\nwGUWSa2v9szkTb4fQcrCTYL7U6SVixq8vAiVYfjExV0smuuOybzjIiXpXqYSVwAA\\r\\nAIEAiU0ecoZ+2eN6Lf9d46jxKbYmj6J675UFIxIeYaPIz4Ev1aznXwsHPg9oog3X\\r\\nlGkIJavmZAgabuiK8yjVwUtCvs1dC0KvRIDhEkVEG6y5c/ExLTf9/uWetlhVdFZQ\\r\\nK13Fln7c/PPILty6YJ7etxW7c2+QhID27IeV5GbwvEjxcpk=\\r\\nPrivate-MAC: a58be360183ea42a152010bb51b38f4277f6871e"
}
}',0;

-- create some extract data for extract 4
insert into data_generator.extract
select 4, 'Subscriber B Health Check', 1, 1, 2, '{
"name": "Data Generator Extract Definition 4",
 "id": "4",
 "projectId": "f0bc6f4a-8f18-11e8-839e-80fa5b320513",
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
          "clientPrivateKey": "PuTTY-User-Key-File-2: ssh-rsa\\r\\nEncryption: none\\r\\nComment: imported-openssh-key\\r\\nPublic-Lines: 6\\r\\nAAAAB3NzaC1yc2EAAAADAQABAAABAQDNb7bfn3hEjjUtnkHCgIeVzUJKRXidXoYP\\r\\na0pzIVEv9Dgo6zN4ajUE2mCCgFi2Y16yJVupRIkFkZ8d31VItkMbrZIyV0wCekDs\\r\\n0I6gxh4V5XJrsiWTHfk4fAajb2AWcjt3Id9Hray8zPoiwE0RxSNcAGHNpR9OO82S\\r\\nndotGj2bOzBXN+16lEWF/bi2ZQ77dYiYKKuzvyyD8MMXJ2E5BTewli4SW8KRxQIl\\r\\nMt1tRfRexUfjDIIn6cyOMs9WCCsr5GC2hQuyyAGNQw2pAxvUvlOYl+WP1h8IPcrS\\r\\nR7lGtmXWfl4hc8zfmR1/jafIkk5SK/W9KpYBHLLQA5EQFZezVQeL\\r\\nPrivate-Lines: 14\\r\\nAAABAFH0rE8ADnnDBcICLZfLsMt5TGXW2yxkxjSmh9fwRbRMyI7CbhEuxaH/AJtv\\r\\nWnTApcmKD8wyVDuNgZ3oN9y/IXyMPROqMd+XKAmRliTbhKsVkxUVx9muDnuTNw7C\\r\\nYuHxhnmbYLj2tz/Gwk9Uyio5rEaKvHnO0vNh9jv0j1KI8mTXevl5Eun9JFpf6bgl\\r\\nRhoobNhBtJSw4vmz352FFO2vTtaqti/idU6AVeB2XWhYDFntaPUfrFmoPMI8VUte\\r\\nhBdPIP1ty/RZQBjaccdugNJmGR08D68LObfOvpr2FH9xRKVh4KIzmaU84n1n011A\\r\\nWd3Vda3m5O6J2DsvLZWsVsGLkkkAAACBAP/aS7s8xPg6GAK7vf9c0f/Z5jYBmoWQ\\r\\nXVLdTbUvuuElgmCvpy6/tPt+fFgFqkOW64fl3RDMM6R1afSoXGH9wtCkG+zTeJam\\r\\n1HtSGQ205qTG2l5TZzIsDqgX75d0+ObIpoHorUmpRBIKz0i5VQrw0EqoTxo18P+h\\r\\nCYsSr0ccBDvtAAAAgQDNjf0kQi0fBr8uIW/dF1CAdnFpbFWsAgecqnxEHHC5wGCM\\r\\nw+tZdpzxNa/Qr1stUdDZvs6XCGs8yB70tQ5Dxac0ZeK6VM6JcF60AQomPe7GsBXH\\r\\nwGUWSa2v9szkTb4fQcrCTYL7U6SVixq8vAiVYfjExV0smuuOybzjIiXpXqYSVwAA\\r\\nAIEAiU0ecoZ+2eN6Lf9d46jxKbYmj6J675UFIxIeYaPIz4Ev1aznXwsHPg9oog3X\\r\\nlGkIJavmZAgabuiK8yjVwUtCvs1dC0KvRIDhEkVEG6y5c/ExLTf9/uWetlhVdFZQ\\r\\nK13Fln7c/PPILty6YJ7etxW7c2+QhID27IeV5GbwvEjxcpk=\\r\\nPrivate-MAC: a58be360183ea42a152010bb51b38f4277f6871e"
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