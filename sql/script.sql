-- Table PAYS
CREATE TABLE pays (
    id_pays SERIAL PRIMARY KEY,
    nom_pays VARCHAR(100) NOT NULL UNIQUE,
    region_oms VARCHAR(50) NOT NULL
);

-- Table MALADIE
CREATE TABLE maladie (
    id_maladie SERIAL PRIMARY KEY,
    nom_maladie VARCHAR(50) NOT NULL UNIQUE,
    type_maladie VARCHAR(50) NOT NULL,
    description TEXT
);

-- Table SITUATION_PANDEMIQUE (table de faits)
CREATE TABLE situation_pandemique (
    id_pays INTEGER,
    id_maladie INTEGER,
    date_observation DATE NOT NULL,
    cas_confirmes INTEGER DEFAULT 0,
    deces INTEGER DEFAULT 0,
    guerisons INTEGER DEFAULT 0,
    cas_actifs INTEGER DEFAULT 0,
    nouveaux_cas INTEGER DEFAULT 0,
    nouveaux_deces INTEGER DEFAULT 0,
    nouvelles_guerisons INTEGER DEFAULT 0,
    -- Clé primaire composite
    PRIMARY KEY (id_pays, id_maladie, date_observation),
    -- Clés étrangères
    FOREIGN KEY (id_pays) REFERENCES pays(id_pays),
    FOREIGN KEY (id_maladie) REFERENCES maladie(id_maladie)
);

-- Création des index pour optimiser les performances
CREATE INDEX idx_situation_date ON situation_pandemique(date_observation);
CREATE INDEX idx_situation_pays ON situation_pandemique(id_pays);
CREATE INDEX idx_situation_maladie ON situation_pandemique(id_maladie);

