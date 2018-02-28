CREATE KEYSPACE IF NOT EXISTS lcmap_local
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS lcmap_local.lcmap_pyccd_2017_10_27(
    chipx int,
    chipy int,
    x int,
    y int,
    dates list<int> STATIC,
    mask list<int>,
    procedure text,
    models text,        
    PRIMARY KEY ((chipx, chipy), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_local.lcmap_pyccd_2017_10_27_training(
    x int,
    y int,
    model text,
    PRIMARY KEY ((x, y))
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_local.lcmap_pyccd_2017_10_27_classification(
    chipx int,
    chipy int,
    x int,
    y int,
    result text,
    PRIMARY KEY ((chipx, chipy), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };
