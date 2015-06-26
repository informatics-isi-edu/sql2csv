{
    "SERVER_NAME":"bugacov.isi.edu",
    "DNS": "gpcr",
    "DATABASE_NAME":"rce",
    "USER_NAME":"sa",
    "CSV_TARGET_DIRECTORY":"extracts",
    "EXTRACTS": [
         { "query_file" : "sql_construct.sql",
	   "schema_name": "gpcr",
	   "table_name" : "construct",
	   "bulk_data_columns" : [],
	   "unique_key_columns": ["id"]
	 },
         { "query_file" : "sql_cont_target.sql",
	   "schema_name": "gpcr",
	   "table_name" : "cont_target",
	   "bulk_data_columns" : [],
	   "unique_key_columns": ["id"]
	 }
    ]
}

