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