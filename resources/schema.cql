CREATE KEYSPACE IF NOT EXISTS ard_cu_c01_v01_aux_cu_v01_ccdc_1_0
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};


/*******************************************************************
 tx:      tile upper left x
 ty:      tile upper left y
 model:   model coefficients
 name:    model name
 updated: timestamp of last model update
*******************************************************************/

CREATE TABLE IF NOT EXISTS ard_cu_c01_v01_aux_cu_v01_ccdc_1_0.tile (
    tx         int,
    ty         int,
    model      text,
    name       text,
    updated    timestamp,
    PRIMARY KEY((tx, ty)))
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND  COMPACTION  = { 'class': 'LeveledCompactionStrategy' };


/*******************************************************************
 cx: upper left x of the chip
 cy: upper left y of the chip
 dates: ARD timestamps for this chip
*******************************************************************/

CREATE TABLE IF NOT EXISTS ard_cu_c01_v01_aux_cu_v01_ccdc_1_0.chip (
    cx         int,
    cy         int,
    dates      frozen<list<date>>,
    PRIMARY KEY((cx, cy)))
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND  COMPACTION  = { 'class': 'LeveledCompactionStrategy' };


/*******************************************************************
 cx:   upper left x of the chip
 cy:   upper left y of the chip
 px:   x pixel coordinate
 py:   y pixel coordinate
 mask: processing mask, 0/1 for not used/used in calculation
       (applies against dates)
*******************************************************************/

CREATE TABLE IF NOT EXISTS ard_cu_c01_v01_aux_cu_v01_ccdc_1_0.pixel (
    cx         int,
    cy         int,
    px         int,
    py         int,
    mask frozen<list<tinyint>>,,
    PRIMARY KEY((cx, cy), px, py))
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND  COMPACTION  = { 'class': 'LeveledCompactionStrategy' };



/*******************************************************************
 If sday and eday are 0, there was no change detected for that location

 cx:     upper left x of the chip
 cy:     upper left y of the chip
 px:     x pixel coordinate
 py:     y pixel coordinate
 sday:   start date
 eday:   end date
 bday:   break date
 chprob: change_probability
 curqa:  curve_qa
 blmag:  blue magnitude
 grmag:  green magnitude
 remag:  red magnitude
 nimag:  nir magnitude
 s1mag:  swir1 magnitude
 s2mag:  swir2 magnitude
 thmag:  thermal magnitude
 blrmse: blue rmse
 grrmse: green rmse
 rermse: red rmse
 nirmse: nir rmse
 s1rmse: swir1 rmse
 s2rmse: swir2 rmse
 thrmse: thermal rmse
 blcoef: blue coefficients
 grcoef: green coefficients
 recoef: red coefficients
 nicoef: nir coefficients
 s1coef: swir1 coefficients
 s2coef: swir2 coefficients
 thcoef: thermal coefficients
 blint:  blue intercept
 grint:  green intercept
 reint:  red intercept
 niint:  nir intercept
 s1int:  swir1 intercept
 s2int:  swir2 intercept
 thint:  thermal intercept
 rfrawp: random forest raw prediction
*******************************************************************/

CREATE TABLE IF NOT EXISTS ard_cu_c01_v01_aux_cu_v01_ccdc_1_0.segment (
    cx     int,
    cy     int,
    px     int,
    py     int,
    sday   date,
    eday   date,
    bday   date,
    chprob float,
    curqa  tinyint,
    blmag  float,
    grmag  float,
    remag  float,
    nimag  float,
    s1mag  float,
    s2mag  float,
    thmag  float,
    blrmse float,
    grrmse float,
    rermse float,
    nirmse float,
    s1rmse float,
    s2rmse float,
    thrmse float,
    blcoef frozen<list<float>>,
    grcoef frozen<list<float>>,
    recoef frozen<list<float>>,
    nicoef frozen<list<float>>,
    s1coef frozen<list<float>>,
    s2coef frozen<list<float>>,
    thcoef frozen<list<float>>,
    blint  float,
    grint  float,
    reint  float,
    niint  float,
    s1int  float,
    s2int  float,
    thint  float,    
    rfrawp frozen<list<float>>,
    PRIMARY KEY((cx, cy), px, py, sday, eday))
    
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND  COMPACTION  = { 'class': 'LeveledCompactionStrategy' };