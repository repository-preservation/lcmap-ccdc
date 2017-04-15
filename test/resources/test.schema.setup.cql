CREATE KEYSPACE IF NOT EXISTS lcmap_changes_local
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS lcmap_changes_local.algorithms(
    algorithm text PRIMARY KEY,
    inputs_url_template text,
    enabled boolean
);

CREATE TABLE IF NOT EXISTS lcmap_changes_local.results(
    tile_x int,
    tile_y int,
    algorithm text,
    x int,
    y int,
    result text,
    result_md5 text,
    result_ok boolean,
    result_produced timestamp,
    tile_update_requested timestamp STATIC,
    inputs_url text STATIC,
    inputs_md5 text STATIC,
    PRIMARY KEY ((tile_x, tile_y, algorithm), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };