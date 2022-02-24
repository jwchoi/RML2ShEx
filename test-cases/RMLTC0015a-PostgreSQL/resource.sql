DROP TABLE IF EXISTS country;

CREATE TABLE country (
  code VARCHAR(2),
  name VARCHAR(100),
  lan VARCHAR(10),
  PRIMARY KEY (code,lan)
);
INSERT INTO country (code, name, lan) VALUES ('BO', 'Bolivia, Plurinational State of', 'EN');
INSERT INTO country (code, name, lan) VALUES ('BO', 'Estado Plurinacional de Bolivia', 'ES');
INSERT INTO country (code, name, lan) VALUES ('IE', 'Ireland', 'EN');
INSERT INTO country (code, name, lan) VALUES ('IE', 'Irlanda', 'ES');
