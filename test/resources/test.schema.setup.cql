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
    production_date timestamp,
    result text,
    result_md5 text,
    result_error text,
    job_conf text STATIC,
    PRIMARY KEY ((chip_x, chip_y, algorithm, product_date), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };
