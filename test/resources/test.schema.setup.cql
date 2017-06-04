CREATE KEYSPACE IF NOT EXISTS lcmap_changes_local
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};


CREATE TABLE IF NOT EXISTS lcmap_changes_local.algorithms(
    algorithm text PRIMARY KEY,
    inputs_url_template text,
    enabled boolean
);


CREATE TABLE IF NOT EXISTS lcmap_changes_local.jobs(
    id uuid,
    submitted timestamp,
    start timestamp,
    end timestamp,
    submitter text,
    jobconf map<text, text>,
    tags = list<text>
    PRIMARY_KEY(id, submitted_by, start_time)
)


CREATE TABLE IF NOT EXISTS lcmap_changes_local.products(
    chip_x int,
    chip_y int,
    algorithm text,
    x int,
    y int,
    product_date text,
    result text,
    result_md5 text,
    result_error text,
    result_produced timestamp,
    inputs_url text STATIC,
    inputs_md5 text STATIC,
    job uuid,
    PRIMARY KEY ((chip_x, chip_y, algorithm), x, y, product_date)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };
