CREATE KEYSPACE data WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
CREATE TABLE data.metrics( id int , uuid varchar, number int, primary key (id));
