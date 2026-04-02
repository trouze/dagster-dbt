import dagster as dg

# Demo partitions map to inbound_file_orders_001/002/003 source tables.
file_partitions = dg.StaticPartitionsDefinition(["001", "002", "003"])
