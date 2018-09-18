from ccdc import cassandra
from ccdc import ids
from ccdc import pyccd
from cytoolz import first
from cytoolz import get
#from .shared import ccd_schema_names
#from .shared import ccd_format_keys
#from .shared import faux_dataframe
#from .shared import mock_cassandra_read
#from .shared import timeseries_element
from pyspark.sql.types import StructType
from pyspark.rdd import PipelinedRDD

import datetime
import pyspark.sql as spark_sql
import test

def test_algorithm():
    assert "lcmap-pyccd" in pyccd.algorithm()

def test_table():
    assert "data" == pyccd.table()

def test_schema():
    assert type(pyccd.schema()) is StructType
    assert set(pyccd.schema().names) == set(test.ccd_schema_names)

def test_dataframe(spark_context, timeseries_rdd):
    rdd    = pyccd.rdd(ctx=spark_context, timeseries=timeseries_rdd)
    dframe = pyccd.dataframe(spark_context, rdd)
    assert set(dframe.columns) == set(test.ccd_schema_names)

def test_default():
    assert pyccd.default([]) == [{'start_day': 1, 'end_day': 1, 'break_day': 1}]
    assert pyccd.default(["foo", "bar"]) == ["foo", "bar"]

def test_format():
    chipx = 100
    chipy = -100
    pixelx = 50
    pixely = -50
    acquired = '1980/2017'
    sday = 1
    eday = 3
    bday = 2
    sdate = datetime.date.fromordinal(sday)
    edate = datetime.date.fromordinal(eday)
    bdate = datetime.date.fromordinal(bday)
    ordinal_dates = [sday, bday, eday]
    iwds_dates = [sdate, bdate, edate]
        
    fval = 0.5

    pyccd_model = {'magnitude': fval,
                   'rmse': fval,
                   'coefficients': (fval, fval),
                   'intercept': fval}    

    pyccd_change_model = {'start_day': sday,
                          'end_day': eday,
                          'break_day': bday,
                          'observation_count': 3,
                          'change_probability': fval,
                          'curve_qa': fval,
                          'blue': pyccd_model,
                          'green': pyccd_model,
                          'red': pyccd_model,
                          'nir': pyccd_model,
                          'swir1': pyccd_model,
                          'swir2': pyccd_model,
                          'thermal': pyccd_model}

    iwds_change_model =  {'cx'     : chipx,
                          'cy'     : chipy,
                          'px'     : pixelx,
                          'py'     : pixely,
                          'sday'   : sdate,
                          'eday'   : edate,
                          'bday'   : bdate, 
                          'chprob' : get('change_probability', pyccd_change_model),
                          'curqa'  : get('curve_qa', pyccd_change_model),
                          'blmag'  : fval,
                          'grmag'  : fval,
                          'remag'  : fval,
                          'nimag'  : fval,
                          's1mag'  : fval,
                          's2mag'  : fval,
                          'thmag'  : fval,
                          'blrmse' : fval, 
                          'grrmse' : fval,
                          'rermse' : fval,
                          'nirmse' : fval,
                          's1rmse' : fval,
                          's2rmse' : fval,
                          'thrmse' : fval,
                          'blcoef' : (fval, fval),
                          'grcoef' : (fval, fval),
                          'recoef' : (fval, fval),
                          'nicoef' : (fval, fval),
                          's1coef' : (fval, fval),
                          's2coef' : (fval, fval),
                          'thcoef' : (fval, fval),
                          'blint'  : fval,
                          'grint'  : fval, 
                          'reint'  : fval,
                          'niint'  : fval, 
                          's1int'  : fval, 
                          's2int'  : fval, 
                          'thint'  : fval, 
                          'dates'  : iwds_dates,
                          'mask' : [0, 1, 0]}

    pyccd_format = pyccd.format(cx = chipx,
                                cy = chipy,
                                px=pixelx,
                                py=pixely,
                                dates=ordinal_dates,
                                ccdresult={'processing_mask': [0, 1, 0],
                                           'change_models': [pyccd_change_model,]})

    print("pyccd_format:{}".format(pyccd_format))
    print("========================================")
    print("\n")
    print("iwds_change_model:{}".format(iwds_change_model))
    
    assert first(pyccd_format) == iwds_change_model
    
    
def test_detect():
    result = pyccd.detect(test.timeseries_element)[0]
    assert result['cx'] == -1815585
    assert set(result.keys()) == set(test.ccd_format_keys)

def test_rdd(spark_context, timeseries_rdd):
    # calling collect, or any other method to realize the results fails
    # unless we monkeypatch the function which actually retrieves chip data.
    # not sure yet if thats beyond scope for this test or not.
    rdd = pyccd.rdd(ctx=spark_context, timeseries=timeseries_rdd)
    assert type(rdd) is PipelinedRDD

#def test_read_write(spark_context, sql_context):
#    # create a dataframe from an rdd
#    rdd       = spark_context.parallelize([(100, -100, 200, -200, 33, 44),
#                                           (300, -300, 400, -400, 55, 66)])
#    layers    = rdd.map(lambda x: spark_sql.Row(chipx=x[0], chipy=x[1], pixelx=x[2], pixely=x[3], sday=x[4], eday=x[5]))
#    context   = spark_sql.SQLContext(spark_context)
#    dataframe = context.createDataFrame(layers)
#
#    # test write
#    written_dataframe = pyccd.write(spark_context, dataframe)
#    assert type(written_dataframe) is spark_sql.dataframe.DataFrame
#
#    # test read
#    ids_rdd   = rdd.map(lambda x: spark_sql.Row(chipx=x[0], chipy=x[1]))
#    ids_df    = ids.dataframe(spark_context, ids_rdd, ids.chip_schema())
#    read_dataframe = pyccd.read(spark_context, ids_df)
#    assert type(read_dataframe) is spark_sql.dataframe.DataFrame
#    assert set([i.asDict()["chipx"] for i in read_dataframe.collect()]) == set([100, 300])


#def test_join(sql_context):
#    df_attrs1 = ['cx', 'cy', 'px', 'py', 'sday', 'eday', 'rfrawp']
#    df_attrs2 = ['cx', 'cy', 'px', 'py', 'sday', 'eday', 'srb3']
#    ccd_df    = faux_dataframe(ctx=sql_context, attrs=df_attrs1)
#    pred_df   = faux_dataframe(ctx=sql_context, attrs=df_attrs2)
#    joined_df = pyccd.join(ccd=ccd_df, predictions=pred_df)
#    assert set(['cx', 'cy', 'px', 'py', 'sday', 'eday', 'srb3']) == set(joined_df.schema.names)

