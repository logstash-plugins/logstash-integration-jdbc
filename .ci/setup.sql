create database jdbc_streaming_db;

\c jdbc_streaming_db;


create table reference_table (
    ip VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL
);


DO $$
    DECLARE
        counter INTEGER := 1 ;
        ipTemplate VARCHAR(50)    := '10.%s.1.1';
        nameTemplate VARCHAR(50) := 'ldn-server-%s';
        locationTemplate VARCHAR(50) := 'LDN-%s-2-3';
    BEGIN

        WHILE counter <= 250
            LOOP
                INSERT INTO reference_table
                VALUES ((SELECT FORMAT(ipTemplate, counter)),
                        (SELECT FORMAT(nameTemplate, counter)),
                        (SELECT FORMAT(locationTemplate, counter)));
                counter = counter + 1;
            END LOOP;
END $$;


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

create DATABASE jdbc_input_db;

\c jdbc_input_db;

CREATE TABLE employee (
    emp_no integer NOT NULL,
    first_name VARCHAR (50) NOT NULL,
    last_name VARCHAR (50) NOT NULL,
    created_at DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at TIMESTAMP
);

INSERT INTO employee VALUES (1, 'David', 'Blenkinsop', '2000-01-02', '2000-02-01 00:30:40');
INSERT INTO employee VALUES (2, 'Mark',  'Guckenheimer', '2000-01-01', '2020-01-01 20:30:40+00');
INSERT INTO employee VALUES (3, 'Ján', 'Borůvka', '2000-02-01', '2020-01-31 20:30:40+00');
INSERT INTO employee VALUES (4, 'Jožko', 'Šuška', '2010-01-01', NULL);
