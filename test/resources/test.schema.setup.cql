CREATE KEYSPACE IF NOT EXISTS lcmap_changes_local
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};


CREATE TABLE IF NOT EXISTS lcmap_changes_local.jobconf (
    id text,
    config text,
    PRIMARY KEY (id)
)
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND COMPACTION = { 'class' : 'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.inputs_20170608(
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


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lcmap_pyccd_2017_06_20(
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


CREATE TABLE IF NOT EXISTS lcmap_changes_local.seglength_20170608(
    chip_x int,
    chip_y int,
    x int,
    y int,
    datestr text,
    result int,
    error text,
    jobconf text,
    PRIMARY KEY ((chip_x, chip_y), datestr, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.changemag_20170608(
    chip_x int,
    chip_y int,
    x int,
    y int,
    datestr text,
    result int,
    error text,
    jobconf text,
    PRIMARY KEY ((chip_x, chip_y), datestr, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.changedate_20170608(
    chip_x int,
    chip_y int,
    x int,
    y int,
    datestr text,
    result int,
    error text,
    jobconf text,
    PRIMARY KEY ((chip_x, chip_y), datestr, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.lastchange_20170608(
    chip_x int,
    chip_y int,
    x int,
    y int,
    datestr text,
    result int,
    error text,
    jobconf text,
    PRIMARY KEY ((chip_x, chip_y), datestr, x, y)
)
WITH COMPRESSION = { 'sstable_compression' : 'LZ4Compressor' }
AND  COMPACTION = { 'class' :  'LeveledCompactionStrategy' };


CREATE TABLE IF NOT EXISTS lcmap_changes_local.curveqa_20170608(
    chip_x int,
    chip_y int,
    x int,
    y int,
    datestr text,
    result int,
    error text,
    jobconf text,
    PRIMARY KEY ((chip_x, chip_y), datestr, x, y)
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
