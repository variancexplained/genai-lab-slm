CREATE TABLE IF NOT EXISTS dataset (
    oid INTEGER NOT NULL,    
    name VARCHAR(32),
    description VARCHAR(128),
    phase VARCHAR(32),
    stage VARCHAR(64),
    size INTEGER,
    nrows INTEGER,
    ncols INTEGER,    
    creator VARCHAR(32),
    created TIMESTAMP,    
    PRIMARY KEY(oid)
);