CREATE TABLE math ( specification TEXT, a INTEGER, b TEXT, c REAL, d INTEGER, e REAL, f REAL);

CREATE TABLE math_units ( specification TEXT, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT);

INSERT INTO math_units VALUES( NULL, NULL, NULL, 'cm', NULL, NULL, NULL);

INSERT INTO math VALUES( '!jack', 1, 'there is CM', 45.98, 2, 34.8, 0.0089);

CREATE TABLE address ( specification TEXT, fileLoc TEXT, g TEXT, h TEXT, i INTEGER, j INTEGER, k INTEGER, l REAL, m INTEGER);

CREATE TABLE address_units ( specification TEXT, fileLoc TEXT, g TEXT, h TEXT, i TEXT, j TEXT, k TEXT, l TEXT, m TEXT);

INSERT INTO address_units VALUES( NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO address VALUES( '!sam', '/home/sam/lib/data', 'good memories', '556place street', 2, 3, 4, 1.0, 99);

CREATE TABLE physics ( specification TEXT, n REAL, o TEXT, p INTEGER, q TEXT, r INTEGER, s REAL);

CREATE TABLE physics_units ( specification TEXT, n TEXT, o TEXT, p TEXT, q TEXT, r TEXT, s TEXT);

INSERT INTO physics_units VALUES( NULL, 'm / s / s', NULL, 's', NULL, 'million grams', NULL);

INSERT INTO physics VALUES( '!amy', 9.8, 'gravity', 23, 'home 23', 1, -0.0012);
