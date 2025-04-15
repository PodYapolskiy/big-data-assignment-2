import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum


def compute_bm25(tf, df, doc_length, avg_dl, N, k1=1.2, b=0.75):
    # BM25 score for one term in one document:
    # BM25 = log((N - df + 0.5)/(df + 0.5)) * ((tf * (k1 + 1)) / (tf + k1 * ((1 - b) + b*(doc_length/avg_dl))))
    idf = math.log((N - df + 0.5) / (df + 0.5))
    numerator = tf * (k1 + 1)
    denominator = tf + k1 * ((1 - b) + b * (doc_length / avg_dl))
    return idf * (numerator / denominator)


def bm25_udf(k1, b, avg_dl, N):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import FloatType

    return udf(
        lambda tf, df, doc_length: (
            float(
                math.log((N - df + 0.5) / (df + 0.5))
                * ((tf * (k1 + 1)) / (tf + k1 * ((1 - b) + b * (doc_length / avg_dl))))
            )
            if tf is not None and df is not None and doc_length is not None
            else 0.0
        ),
        FloatType(),
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: query.py "<your query>"')
        sys.exit(1)

    query_text = sys.argv[1]

    # Tokenize the query; the same simple rule as in the MapReduce job
    query_terms = [term for term in query_text.lower().split() if term]

    # Create Spark session with Cassandra connector settings
    spark = (
        SparkSession.builder.appName("BM25_Query")
        .config("packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.cassandra.connection.host", "host.docker.internal")
        # cassandra-server
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )

    # Read document statistics from Cassandra table "doc_stats" in keyspace "search"
    doc_stats = (
        spark.read.format("cassandra")
        .options(table="doc_stats", keyspace="search")
        .load()
        .select(col("doc_id"), col("doc_title"), col("doc_length"))
    )
    print(doc_stats)
    sys.exit(0)

    # Read inverted index from Cassandra table "inverted_index"
    inv_index = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="inverted_index", keyspace="search")
        .load()
        .select(col("term"), col("doc_id"), col("tf"))
    )

    # Read vocabulary from Cassandra table "vocabulary"
    vocab = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="vocabulary", keyspace="search")
        .load()
        .select(col("term"), col("df"))
    )

    # Compute total number of documents N and average document length
    N = doc_stats.count()
    avg_dl = doc_stats.agg(spark_sum("doc_length")).first()[0] / N

    # Filter the inverted index for query terms only.
    query_index = inv_index.filter(col("term").isin(query_terms))
    # Join with vocabulary table to get df for each term.
    query_index = query_index.join(vocab, on="term", how="left")
    # Join with doc_stats to get document lengths and titles.
    query_index = query_index.join(doc_stats, on="doc_id", how="left")

    # Set BM25 hyperparameters
    k1 = 1.0
    b = 0.75

    # Register the BM25 UDF
    bm25 = bm25_udf(k1, b, avg_dl, N)
    query_index = query_index.withColumn(
        "bm25", bm25(col("tf"), col("df"), col("doc_length"))
    )
    # Sum the BM25 score for each document (if more than one query term matches)
    scores = (
        query_index.groupBy("doc_id", "doc_title")
        .agg({"bm25": "sum"})
        .withColumnRenamed("sum(bm25)", "bm25_score")
    )

    # Retrieve top 10 documents by BM25 score
    top_docs = scores.orderBy(col("bm25_score").desc()).limit(10)

    print("Query: " + query_text)
    top = top_docs.collect()
    print("Top Documents (doc_id, title, BM25 score):")
    for row in top:
        print(
            "{0}\t{1}\t{2:.4f}".format(
                row["doc_id"], row["doc_title"], row["bm25_score"]
            )
        )

    spark.stop()
