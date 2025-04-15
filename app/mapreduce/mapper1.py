import re
import sys
import unicodedata
import pymorphy2
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

nltk.download("stopwords", quiet=True)
nltk.download("wordnet", quiet=True)

# Initialize stop words and lemmatizer
stop_words = set(stopwords.words("english")) | set(stopwords.words("russian"))
lemmatizer = WordNetLemmatizer()
morph = pymorphy2.MorphAnalyzer()


def normalize_text(text):
    """
    Normalizes unicode text by decomposing characters (NFKD)
    and removing diacritical marks.
    E.g. 'español' becomes 'espanol'.
    """
    normalized = unicodedata.normalize("NFKD", text)
    # Filter out diacritical marks (combining characters)
    normalized = "".join([c for c in normalized if not unicodedata.combining(c)])
    return normalized


def lemmatize_word(word):
    """
    Lemmatises a word using a different backend depending on its script.
    If the word contains any Cyrillic letters, use pymorphy2;
    otherwise assume a Latin-script word and use spaCy.
    """
    if re.search(r"[\u0400-\u04FF]", word):
        # Use pymorphy2 for Cyrillic words (e.g., Russian)
        parsed = morph.parse(word)
        if parsed:
            return parsed[0].normal_form
        else:
            return word
    else:
        return lemmatizer.lemmatize(word)


def tokenize(text: str):
    text = text.strip().lower()
    text = normalize_text(text)  # Normalize text (e.g., "español" becomes "espanol")

    # Define a regex pattern to match tokens containing only Latin or Cyrillic letters.
    valid_letters_pattern = re.compile(r"^[A-Za-z\u0400-\u04FF]+$")

    tokens = []
    for token in text.split(" "):
        token = re.sub(r"[^\w]", "", token)  # remove punctuation from toke

        if not token or token in stop_words:
            continue

        if token.isdigit():  # skip digits
            # tokens.append(token)
            continue

        if valid_letters_pattern.fullmatch(token):
            token_lower = token.lower()
            lemma = lemmatize_word(token_lower)
            tokens.append(lemma)

    return tokens


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
        sys.exit(1)
