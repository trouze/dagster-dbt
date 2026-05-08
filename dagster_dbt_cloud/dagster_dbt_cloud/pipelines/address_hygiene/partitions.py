import dagster as dg

file_partitions = dg.DynamicPartitionsDefinition(name="file_partitions")
