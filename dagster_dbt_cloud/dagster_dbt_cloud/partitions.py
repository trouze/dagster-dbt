import dagster as dg

FILE_PARTITIONS_NAME = "file_partitions"

file_partitions = dg.DynamicPartitionsDefinition(name=FILE_PARTITIONS_NAME)
