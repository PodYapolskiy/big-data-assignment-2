import re
import sys


def tokenize(text: str):
    # Basic tokenization: lowercase and split on non-alphanumeric characters.
    tokens = re.split(r"\W+", text.lower())
    return [token for token in tokens if token]


def process_line(line: str):
    # Each line is expected to have: <doc_id> <tab> <doc_title> <tab> <doc_text>
    try:
        doc_id, doc_title, doc_text = line.strip().split("\t", 2)
    except ValueError:
        # Skip malformed lines
        sys.stderr.write("Malformed input line: {}\n".format(line))
        return

    # text preprocessing on tokens
    tokens = tokenize(doc_text)

    # Emit document statistics record.
    # Format: DOC|<doc_id>      <doc_title>      <doc_length>
    sys.stdout.write(f"DOC|{doc_id}\t{doc_title}\t{len(tokens)}\n")

    # Count term frequencies for the document.
    tf_dict = {}
    for token in tokens:
        tf_dict[token] = tf_dict.get(token, 0) + 1

    # Emit one record per term in the document.
    # Format: TERM|<term>     <doc_id>    <tf>
    for term, tf in tf_dict.items():
        sys.stdout.write(f"TERM|{term}|{doc_id}\t{tf}\n")


for line in sys.stdin:
    try:
        process_line(line)
    except Exception as e:
        sys.stderr.write(f"{e}")
