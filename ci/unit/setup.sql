create database jdbc_static_db;

\c jdbc_static_db;


create table reference_table (
    ip VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL
);


INSERT INTO reference_table VALUES ('10.1.1.1', 'ldn-server-1', 'LDN-2-3-4');
INSERT INTO reference_table VALUES ('10.2.1.1', 'nyc-server-1', 'NYC-5-2-8');
INSERT INTO reference_table VALUES ('10.3.1.1', 'mv-server-1', 'MV-9-6-4');
