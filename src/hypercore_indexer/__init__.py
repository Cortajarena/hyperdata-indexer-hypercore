"""HyperCore indexer — batch ingestion of hyperdata-node output files.

Reads the node's hour-files, normalises each line into contract-conformant
rows, and appends them to Apache Iceberg. The row contract is NOT defined here:
it is generated from `platform/schemas` (see contract.py).
"""

__version__ = "0.1.0"
