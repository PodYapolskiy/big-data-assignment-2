#!/app/.venv/bin/python3
# -*-coding:utf-8 -*
import sys
from cassandra.cluster import Cluster

# Connect to Cassandra (each reducer node does its own connection)
try:
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("search")
except Exception as e:
    sys.stderr.write("Error connecting to Cassandra: {}\n".format(e))
    sys.exit(1)

# Prepare the CQL statements
stmt_doc = session.prepare(
    "INSERT INTO doc_stats (doc_id, doc_title, doc_length) VALUES (?, ?, ?)"
)
stmt_index = session.prepare(
    "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)"
)
stmt_vocab_init = session.prepare("UPDATE vocabulary SET df = df + 0 WHERE term = %s")
stmt_vocab_update = session.prepare(
    "UPDATE vocabulary SET df = df + %s WHERE term = %s"
)


def insert_doc_stats(doc_id: int, doc_title: str, doc_length: int) -> None:
    try:
        session.execute(stmt_doc, (doc_id, doc_title, doc_length))
    except Exception as e:
        sys.stderr.write(f"Error inserting doc_stats for {doc_id}:\n{e}\n")


def insert_term_docs_info(term: str, doc_id: str, tf: int) -> None:
    try:
        session.execute(stmt_index, (term, doc_id, tf))
    except Exception as e:
        sys.stderr.write(
            f"Error inserting inverted_index for term {term} doc {doc_id}: {e}\n"
        )

    # Insert vocabulary info for the term (document frequency)
    # df = len(postings)
    # try:
    #     session.execute(stmt_vocab, (term, df))
    # except Exception as e:
    #     sys.stderr.write("Error inserting vocabulary for term {}: {}\n".format(term, e))


def main():
    initialized_terms = set()

    # Read input from file produced by mapper
    temp_file = open("/tmp/index", "r")

    # Process each input line from mapper output.
    for line in temp_file:
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

        # facing term mapping output TERM|term|doc_id<tab>tf
        elif key.startswith("TERM|"):
            parts = key.split("|")
            if len(parts) != 3:
                continue

            _, term, doc_id = parts
            try:
                doc_id = int(doc_id)
                tf = int(value)
            except ValueError:
                continue

            # Initialize the term in the vocabulary.
            if term not in initialized_terms:
                try:
                    session.execute(stmt_vocab_init, (term))
                    initialized_terms.add(term)
                except Exception as e:
                    sys.stderr.write(
                        f"Error initializing vocabulary for term {term}: {e}\n"
                    )
                    continue

            # update the document frequency for the term
            try:
                session.execute(stmt_vocab_update, (tf, term))
            except Exception as e:
                sys.stderr.write(f"Error updating vocabulary for term {term}: {e}\n")
                continue

            # insert term doc info
            try:
                session.execute(stmt_index, (term, doc_id, tf))
            except Exception as e:
                sys.stderr.write(
                    f"Error processing TERM record for term {term} in doc {doc_id}: {e}\n"
                )


if __name__ == "__main__":
    main()
