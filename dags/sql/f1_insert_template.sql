-- Criação do Schema caso não exista
CREATE SCHEMA IF NOT EXISTS f1_schema;

-- Criação da Tabela Final Consolidada
CREATE TABLE IF NOT EXISTS f1_schema.results (
    season INT,
    round INT,
    race_name VARCHAR(255),
    race_date DATE,
    driverId VARCHAR(255),
    driver_name VARCHAR(255),
    statusId FLOAT,
    status VARCHAR(255)
);

-- Limpa os dados antigos para evitar duplicidade em reprocessamentos
TRUNCATE TABLE f1_schema.results;
