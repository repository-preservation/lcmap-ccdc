CREATE KEYSPACE IF NOT EXISTS ccdc_1_0
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

/* If sday and eday are 0, there was no change detected for that location

 Data dictionary:

 chipx:  upper left x of the chip
 chipy:  upper left y of the chip
 x:      x coordinate
 y:      y coordinate
 sday:   start day
 eday:   end day
 bday:   break day
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
 dates:  ARD dates for this chip (static)
 snprob: snow probability
 waprob: water probability
 clprob: cloud probability
 prmask: processing mask, 0/1 for not used/used in calculation (applies against dates)
 rfrawp: random forest raw prediction
*/

CREATE TABLE IF NOT EXISTS CCDC_1_0.data (
    chipx  int,
    chipy  int,
    x      int,
    y      int,
    sday   int,
    eday   int,
    bday   int,
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
    dates  frozen<list<int>> static,
    snprob float,
    waprob float,
    clprob float,
    prmask frozen<list<tinyint>>,
    rfrawp frozen<list<float>>,
    PRIMARY KEY((chipx, chipy), x, y, sday, eday)
)
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND  COMPACTION  = { 'class': 'LeveledCompactionStrategy' };


-------
/* If sday and eday are 0, there was no change detected for that location

 Data dictionary:
 ulx:            tile upper left x
 uly:            tile upper left y
 lrx:            tile lower right x
 lry:            tile lower right y
 h:              tile h designation
 v:              tile v designation
 acq:            date range filter for input observations
 detector:       blue magnitude
 dran:           timestamp detection was run 
 segcnt:         number of change segments identified (including 1 non detection per x,y)
 classifier:     red magnitude
 cran:           timestamp classifier was run
 ardurl:         swir1 magnitude
 auxurl:         swir2 magnitude
 
*/

CREATE TABLE IF NOT EXISTS CCDC_1_0.metadata (
    ulx        int,
    uly        int,
    lrx        int,
    lry        int,
    h          int,
    v          int,
    acq        text,
    detector   text,
    dran       text,
    segcnt     int,
    classifier text,
    cran       text,
    ardurl     text,
    auxurl     text,
    PRIMARY KEY((ulx, uly))
)
WITH COMPRESSION = { 'sstable_compression': 'LZ4Compressor' }
AND  COMPACTION  = { 'class': 'LeveledCompactionStrategy' };