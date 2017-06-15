CREATE KEYSPACE IF NOT EXISTS lcmap_changes_local
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};


CREATE TABLE IF NOT EXISTS lcmap_changes_local.jobconf (
    id text,
    config text,
    PRIMARY KEY (id)
)
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND COMPACTION = { 'class' : 'LeveledCompactionStrategy' }


CREATE TABLE IF NOT EXISTS lcmap_changes_local.inputs_2017_06_08(
    chip_x int,
    chip_y int,
    x int,
    y int,
    result text,
    jobconf_id text,
    PRIMARY KEY ((chip_x, chip_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_pyccd_2017_06_08(
    chip_x int,
    chip_y int,
    x int,
    y int,
    result text,
    jobconf_id text,
    PRIMARY KEY ((chip_x, chip_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_seglength_2017_06_08(
    chip_x int,
    chip_y int,
    x int,
    y int,
    date timestamp,
    result int,
    jobconf_id text,
    PRIMARY KEY ((chip_x, chip_y), date, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_changemag_2017_06_08(
    chip_x int,
    chip_y int,
    x int,
    y int,
    date timestamp,
    result int,
    jobconf_id text,
    PRIMARY KEY ((chip_x, chip_y), date, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_changedate_2017_06_08(
    chip_x int,
    chip_y int,
    x int,
    y int,
    date timestamp,
    result int,
    jobconf_id text,
    PRIMARY KEY ((chip_x, chip_y), date, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_lastchange_2017_06_08(
    chip_x int,
    chip_y int,
    x int,
    y int,
    date timestamp,
    result int,
    jobconf_id text,
    PRIMARY KEY ((chip_x, chip_y), date, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_curveqa_2017_06_08(
    chip_x int,
    chip_y int,
    x int,
    y int,
    date timestamp,
    result int,
    jobconf_id text,
    PRIMARY KEY ((chip_x, chip_y), date, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.training(
    chip_x int,
    chip_y int,
    x int,
    y int,
    model text,
    PRIMARY KEY ((chip_x, chip_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.classification(
    chip_x int,
    chip_y int,
    x int,
    y int,
    result text,
    PRIMARY KEY ((chip_x, chip_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE KEYSPACE IF NOT EXISTS lcmap_ancillary_local
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS lcmap_ancillary_local.trends(
    chip_x int,
    chip_y int,
    x int,
    y int,
    result float,
    PRIMARY KEY ((chip_x, chip_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_ancillary_local.dem(
    chip_x int,
    chip_y int,
    x int,
    y int,
    result float,
    PRIMARY KEY ((chip_x, chip_y), x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };
