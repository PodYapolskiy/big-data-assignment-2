import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


# // WITH CLUSTERING ORDER BY (term ASC);
# // CREATE INDEX IF NOT EXISTS inverted_index_doc_id_idx ON inverted_index(doc_id);
# // CREATE INDEX IF NOT EXISTS vocabulary_term_idx ON vocabulary(term);

############
# KEYSPACE #
############
search_create_statement = """
CREATE KEYSPACE IF NOT EXISTS search WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
"""

##########
# TABLES #
##########
doc_stats_create_statement = """
CREATE TABLE IF NOT EXISTS doc_stats (
    doc_id int,
    doc_title text,
    doc_length int,
    PRIMARY KEY (doc_id)
);
"""

#####################
# INSERTS / UPDATES #
#####################
inverted_index_create_statement ="""
CREATE TABLE IF NOT EXISTS inverted_index (
    term text,
    doc_id int,
    tf int,
    PRIMARY KEY (term, doc_id)
);
"""

vocabulary_create_statement = """
CREATE TABLE IF NOT EXISTS vocabulary (
    term text,
    df counter,
    PRIMARY KEY (term)
);
"""

# Connect to Cassandra (each reducer node does its own connection)
try:
    cluster = Cluster(
        ["host.docker.internal"],
        port=9042,
        auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"),
    )
    # connect and create keyspace if not exists
    session = cluster.connect()
    session.execute(search_create_statement)

    # create table if not exist
    session = cluster.connect("search")
    session.execute(doc_stats_create_statement)
    session.execute(inverted_index_create_statement)
    session.execute(vocabulary_create_statement)
except Exception as e:
    sys.stderr.write(f"Error connecting to Cassandra: {e}\n")
    sys.exit(1)

#####################
# INSERTS / UDPATES #
#####################
doc_stats_insert_statement = session.prepare("INSERT INTO doc_stats (doc_id, doc_title, doc_length) VALUES (?, ?, ?)")
inverted_index_insert_statement = session.prepare("INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")
vocabulary_init_statement = session.prepare("UPDATE vocabulary SET df = df + 0 WHERE term = ?")
vocabulary_update_statement = session.prepare("UPDATE vocabulary SET df = df + 1 WHERE term = ?")


def insert_doc_stats(doc_id: int, doc_title: str, doc_length: int):
    try:
        session.execute(doc_stats_insert_statement, (doc_id, doc_title, doc_length))
    except Exception as e:
        sys.stderr.write(f"Error inserting doc_stats for {doc_id}:\n{e}\n")


def insert_term_docs_info(term: str, doc_id: str, tf: int):
    try:
        session.execute(inverted_index_insert_statement, (term, doc_id, tf))
    except Exception as e:
        sys.stderr.write(
            f"Error inserting inverted_index for term {term} doc {doc_id}: {e}\n"
        )

def main():
    initialized_terms = set()

    # Process each input line from mapper output.
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            key, value = line.split("\t", 1)
        except ValueError:
            continue

        # facing string with document mapping output DOC|doc_id<tab>doc_title<tab>doc_length
        if key.startswith("DOC|"):
            # Process document record immediately.
            try:
                doc_id = int(key.split("|")[1])
                doc_title = value.split("\t")[0]
                doc_length = int(value.split("\t")[1])

                insert_doc_stats(doc_id, doc_title, doc_length)
            except Exception as e:
                sys.stderr.write("Error processing DOC record: {}\n".format(e))
                continue

        # facing term mapping output TERM|term|doc_id<tab>tf
        elif key.startswith("TERM|"):
            parts = key.split("|", 2)
            if len(parts) != 3:
                continue

            _, term, doc_id = parts
            try:
                doc_id = int(doc_id)
                tf = int(value)
            except ValueError as e:
                sys.stderr.write(f"{e}")
                continue

            # Initialize the term in the vocabulary.
            if term not in initialized_terms:
                try:
                    # session.execute(vocabulary_init_statement, (term,))
                    initialized_terms.add(term)
                except Exception as e:
                    sys.stderr.write(
                        f"Error initializing vocabulary for term {term}: {e}\n"
                    )
                    continue

            # update the document frequency for the term
            try:
                session.execute(vocabulary_update_statement, (term,))
            except Exception as e:
                sys.stderr.write(f"Error updating vocabulary for term {term}: {e}\n")
                continue

            # insert term doc info
            try:
                session.execute(inverted_index_insert_statement, (term, doc_id, tf))
            except Exception as e:
                sys.stderr.write(
                    f"Error processing TERM record for term {term} in doc {doc_id}: {e}\n"
                )


main()
