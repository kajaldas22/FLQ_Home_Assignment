--Example: iceberg_ table DDL with partitioning.

Create table iceberg_catalog.db.journal_entries (
  entry_id string ,
  entry_Date date ,
  account_id string,
  debit double,
  credit double,
  year int,
  month int
)
PARTITION   by (year, month)
USING  ICEBERG;

--Compaction : compact\merging small files for the tables.

CALL iceberg_catalog.system.rewrite_data_files(
            table=>'db.journal_entries',
            options =>map('min-input-files','2',
                'max-file-size-bytes' , '536870912'
            }
          )

--retention: remove older snapshot to free up storage
CALL iceberg_catalog.system.expire_snapshots (
             table=>'db.journal_entries',
             older_than=> TIMESTAMP 'NOW' - INTERVAL '30 DAYS',
             retain_last =>5
   
);