CREATE TABLE test
(
       id int unsigned NOT NULL auto_increment,
       code varchar(60) NOT NULL,
       text varchar(255) NOT NULL,
       is_test boolean NOT NULL,
       created_at datetime NOT NULL,
       CONSTRAINT UQ_code UNIQUE (code),
       PRIMARY KEY (id)
);

