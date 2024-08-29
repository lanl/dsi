CREATE TABLE math ( specification TEXT, a INTEGER, b TEXT, c REAL, d INTEGER, e REAL, f TEXT);

CREATE TABLE math_units ( specification TEXT, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT);

INSERT INTO math_units ('specification', 'a', 'b', 'c', 'd', 'e', 'f') VALUES( NULL, NULL, NULL, 'cm', NULL, NULL, NULL);

INSERT INTO math ('specification', 'a', 'b', 'c', 'd', 'e', 'f') VALUES( '!jack', 1, 'there is CM', 45.98, 2, 34.8, '89e4');

CREATE TABLE address ( specification TEXT, fileLoc TEXT, g TEXT, h TEXT, i INTEGER, j INTEGER, k INTEGER, l TEXT, m INTEGER);

CREATE TABLE address_units ( specification TEXT, fileLoc TEXT, g TEXT, h TEXT, i TEXT, j TEXT, k TEXT, l TEXT, m TEXT);

INSERT INTO address_units ('specification', 'fileLoc', 'g', 'h', 'i', 'j', 'k', 'l', 'm') VALUES( NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO address ('specification', 'fileLoc', 'g', 'h', 'i', 'j', 'k', 'l', 'm') VALUES( '!sam', '/home/sam/lib/data', 'good memories', '556place street', 2, 3, 4, '10000e-4', 99);

CREATE TABLE physics ( specification TEXT, n REAL, o TEXT, p INTEGER, q TEXT, r INTEGER, s TEXT);

CREATE TABLE physics_units ( specification TEXT, n TEXT, o TEXT, p TEXT, q TEXT, r TEXT, s TEXT);

INSERT INTO physics_units ('specification', 'n', 'o', 'p', 'q', 'r', 's') VALUES( NULL, 'm / s / s', NULL, 's', NULL, 'million grams', NULL);

INSERT INTO physics ('specification', 'n', 'o', 'p', 'q', 'r', 's') VALUES( '!amy', 9.8, 'gravity', 23, 'home 23', 1, '-12e-4');

