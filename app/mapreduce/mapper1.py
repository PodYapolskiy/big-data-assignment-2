import re
import sys


def tokenize(text: str) -> list[str]:
    # Basic tokenization: lowercase and split on non-alphanumeric characters.
    tokens = re.split(r"\W+", text.lower())
    return [token for token in tokens if token]


def process_line(line):
    # Each line is expected to have: <doc_id> <tab> <doc_title> <tab> <doc_text>
    try:
        doc_id, doc_title, doc_text = line.strip().split("\t", 2)
    except ValueError:
        # Skip malformed lines
        return

    # text preprocessing on tokens
    tokens = tokenize(doc_text)

    # Write output to a temporary file.
    temp_file = open("/tmp/index", "a+")

    # Emit document statistics record.
    # Format: DOC|<doc_id>      <doc_title>      <doc_length>
    print(f"DOC|{doc_id}\t{doc_title}\t{len(tokens)}", file=temp_file, flush=True)

    # Count term frequencies for the document.
    tf_dict = {}
    for token in tokens:
        tf_dict[token] = tf_dict.get(token, 0) + 1

    # Emit one record per term in the document.
    # Format: TERM|<term>     <doc_id>    <tf>
    for term, tf in tf_dict.items():
        print(f"TERM|{term}|{doc_id}\t{tf}", file=temp_file, flush=True)

    # finally free file
    temp_file.close()


if __name__ == "__main__":
    for line in sys.stdin:
        process_line(line)
