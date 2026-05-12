import dagster as dg

# Static partition IDs matching the inbound_file_orders_NNN table suffix.
# Add new IDs here as new source tables arrive; switch to DynamicPartitionsDefinition
# once table discovery via Snowflake SHOW TABLES is wired up.
file_partitions = dg.StaticPartitionsDefinition(["001", "002", "003"])
