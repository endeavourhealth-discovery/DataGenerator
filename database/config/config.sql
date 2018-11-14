use config;

insert into config (app_id, config_id, config_data)
values ('data-generator','database',
      '{
   "url" : "jdbc:mysql://localhost:3306/data_generator",
   "username" : "root",
   "password" : "password"
}'
);