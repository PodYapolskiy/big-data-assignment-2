CREATE KEYSPACE IF NOT EXISTS search WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE search;

CREATE TABLE IF NOT EXISTS doc_stats (
    doc_id int,
    doc_title text,
    doc_length int,
    PRIMARY KEY (doc_id)
);

CREATE TABLE IF NOT EXISTS inverted_index (
    term text,
    doc_id int,
    tf int,
    PRIMARY KEY (term, doc_id)
) WITH CLUSTERING ORDER BY (term ASC);

// CREATE INDEX IF NOT EXISTS inverted_index_doc_id_idx ON inverted_index(doc_id);

CREATE TABLE IF NOT EXISTS vocabulary (
    term text,
    df int,
    PRIMARY KEY (term)
);
