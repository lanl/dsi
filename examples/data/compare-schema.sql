CREATE TABLE math ( specification VARCHAR, a INT, b VARCHAR, c FLOAT, d INT, e FLOAT, f FLOAT);

CREATE TABLE math_units ( specification VARCHAR, a VARCHAR, b VARCHAR, c VARCHAR, d VARCHAR, e VARCHAR, f VARCHAR);

INSERT INTO math_units VALUES( NULL, NULL, NULL, 'cm', NULL, NULL, NULL);

INSERT INTO math VALUES( '!jack', 1, 'there is CM', 45.98, 2, 34.8, 0.0089);

CREATE TABLE address ( specification VARCHAR, fileLoc VARCHAR, g VARCHAR, h VARCHAR, i INT, j INT, k INT, l FLOAT, m INT);

CREATE TABLE address_units ( specification VARCHAR, fileLoc VARCHAR, g VARCHAR, h VARCHAR, i VARCHAR, j VARCHAR, k VARCHAR, l VARCHAR, m VARCHAR);

INSERT INTO address_units VALUES( NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO address VALUES( '!sam', '/home/sam/lib/data', 'good memories', '556place street', 2, 3, 4, 1.0, 99);

CREATE TABLE physics ( specification VARCHAR, n FLOAT, o VARCHAR, p INT, q VARCHAR, r INT, s FLOAT);

CREATE TABLE physics_units ( specification VARCHAR, n VARCHAR, o VARCHAR, p VARCHAR, q VARCHAR, r VARCHAR, s VARCHAR);

INSERT INTO physics_units VALUES( NULL, 'm / s / s', NULL, 's', NULL, 'million grams', NULL);

INSERT INTO physics VALUES( '!amy', 9.8, 'gravity', 23, 'home 23', 1, -0.0012);
