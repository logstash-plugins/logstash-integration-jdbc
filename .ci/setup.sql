create database jdbc_streaming_db;

\c jdbc_streaming_db;


create table reference_table (
    ip VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL,
    entry_date DATE NOT NULL,
    entry_time TIME NOT NULL,
    timestamp TIMESTAMP NOT NULL
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
                        (SELECT FORMAT(locationTemplate, counter)),
                        '2003-02-01',
                        '10:05:00',
                        '2003-02-01 01:02:03');
                counter = counter + 1;
            END LOOP;
END $$;


create database jdbc_static_db;

\c jdbc_static_db;


create table reference_table (
    ip VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL,
    entry_date DATE NOT NULL,
    entry_time TIME NOT NULL,
    timestamp TIMESTAMP NOT NULL
);


INSERT INTO reference_table VALUES ('10.1.1.1', 'ldn-server-1', 'LDN-2-3-4','2003-02-01', '10:05:00', '2003-02-01 01:02:03');
INSERT INTO reference_table VALUES ('10.2.1.1', 'nyc-server-1', 'NYC-5-2-8','2003-02-01', '10:05:00', '2003-02-01 01:02:03');
INSERT INTO reference_table VALUES ('10.3.1.1', 'mv-server-1', 'MV-9-6-4', '2003-02-01', '10:05:00', '2003-02-01 01:02:03');

create DATABASE jdbc_input_db;

\c jdbc_input_db;

CREATE TABLE employee (
    emp_no integer NOT NULL,
    first_name VARCHAR (50) NOT NULL,
    last_name VARCHAR (50) NOT NULL,
    entry_date DATE NOT NULL,
    entry_time TIME NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

INSERT INTO employee VALUES (1, 'David', 'Blenkinsop', '2003-02-01', '10:05:00', '2003-02-01 01:02:03');
INSERT INTO employee VALUES (2, 'Mark', 'Guckenheimer', '2003-02-01', '10:05:00','2003-02-01 01:02:03');
