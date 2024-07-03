CREATE TABLE IF NOT EXISTS backup (
    oid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,    
    backup_type VARCHAR(16) NOT NULL,
    backup_source VARCHAR(255) NOT NULL,
    backup_filename VARCHAR(255) NOT NULL,    
    nfiles INTEGER NOT NULL,
    ndirectories INTEGER NOT NULL,
    size_compressed INTEGER NOT NULL,
    size_uncompressed INTEGER NOT NULL,
    created TIMESTAMP,        
);