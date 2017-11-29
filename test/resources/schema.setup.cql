CREATE KEYSPACE IF NOT EXISTS lcmap_changes_local
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};


CREATE TABLE IF NOT EXISTS lcmap_changes_local.jobconf (
    id text,
    config text,
    PRIMARY KEY (id)
)
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND COMPACTION = { 'class' : 'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_pyccd_2017_10_27(
    chip_x int,
    chip_y int,
    x int,
    y int,
    datestr text,
    result text,
    error text,
    jobconf text,
    PRIMARY KEY ((chip_x, chip_y), datestr, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_training(
    tile_x int,
    tile_y int,
    model text,
    PRIMARY KEY ((tile_x, tile_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_classification(
    chip_x int,
    chip_y int,
    x int,
    y int,
    result text,
    PRIMARY KEY ((chip_x, chip_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };
