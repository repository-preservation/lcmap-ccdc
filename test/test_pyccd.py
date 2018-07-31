from ccdc import cassandra
from ccdc import ids
from ccdc import pyccd
from .shared import ccd_schema_names
from .shared import ccd_format_keys
from .shared import faux_dataframe
from .shared import mock_cassandra_read
from .shared import pyccd_change_model
from .shared import iwds_change_model
from .shared import timeseries_element
from pyspark.sql.types import StructType
from pyspark.rdd import PipelinedRDD

import pyspark.sql as spark_sql


def test_algorithm():
    assert "lcmap-pyccd" in pyccd.algorithm()

def test_table():
    assert "data" == pyccd.table()

def test_schema():
    assert type(pyccd.schema()) is StructType
    assert set(pyccd.schema().names) == set(ccd_schema_names)

def test_dataframe(spark_context, timeseries_rdd):
    rdd    = pyccd.rdd(ctx=spark_context, timeseries=timeseries_rdd)
    dframe = pyccd.dataframe(spark_context, rdd)
    assert set(dframe.columns) == set(ccd_schema_names)

def test_default():
    assert pyccd.default([]) == [{'start_day': 0, 'end_day': 0}]
    assert pyccd.default(["foo", "bar"]) == ["foo", "bar"]

def test_format():
    chipx = 100
    chipy = -100
    pixelx= = 50
    pixely = -50
    acquired = '1980/2017'
    fval = 0.5
    
    pyccd_model = {magnitude: fval,
                   rmse: fval,
                   coefficients: (fval, fval),
                   intercept: fval}    

    pyccd_change_model = {start_day: 1,
                          end_day: 3,
                          break_day: 2,
                          observation_count: 3,
                          change_probability: fval,
                          curve_qa: fval,
                          blue: pyccd_model,
                          green: pyccd_model,
                          red: pyccd_model,
                          nir: pyccd_model,
                          swir1: pyccd_model,
                          swir2: pyccd_model,
                          thermal: pyccd_model,
                          snow_prob: fval,
                          water_prob: fval,
                          cloud_prob: fval,
                          processing_mask: [0, 1, 0]}

    iwds_change_model =  {'chipx'  : chipx,
                          'chipy'  : chipy,
                          'pixelx' : pixelx,
                          'pixely' : pixely,
                          'sday'   : get('start_day', pyccd_change_model)
                          'eday'   : get('end_day', pyccd_change_model),
                          'bday'   : get('break_day', pyccd_change_model),
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
                          'dates'  : dates,
                          'snprob' : fval, 
                          'waprob' : fval, 
                          'clprob' : fval, 
                          'prmask' : [0, 1, 0]}

    pyccd_format = pyccd.format(chipx, chipy, pixelx, pixely,
                                acquired, {'change_models': [pyccd_change_model,]})
    
    assert pyccd_format == iwds_change_model
    
    
def test_detect():
    result = pyccd.detect(timeseries_element)[0]
    assert result['chipx'] == -1815585
    assert set(result.keys()) == set(ccd_format_keys)

def test_rdd(spark_context, timeseries_rdd):
    # calling collect, or any other method to realize the results fails
    # unless we monkeypatch the function which actually retrieves chip data.
    # not sure yet if thats beyond scope for this test or not.
    rdd = pyccd.rdd(ctx=spark_context, timeseries=timeseries_rdd)
    assert type(rdd) is PipelinedRDD

def test_read_write(spark_context, sql_context):
    # create a dataframe from an rdd
    rdd       = spark_context.parallelize([(100, -100, 200, -200, 33, 44),
                                           (300, -300, 400, -400, 55, 66)])
    layers    = rdd.map(lambda x: spark_sql.Row(chipx=x[0], chipy=x[1], pixelx=x[2], pixely=x[3], sday=x[4], eday=x[5]))
    context   = spark_sql.SQLContext(spark_context)
    dataframe = context.createDataFrame(layers)

    # test write
    written_dataframe = pyccd.write(spark_context, dataframe)
    assert type(written_dataframe) is spark_sql.dataframe.DataFrame

    # test read
    ids_rdd   = rdd.map(lambda x: spark_sql.Row(chipx=x[0], chipy=x[1]))
    ids_df    = ids.dataframe(spark_context, ids_rdd)
    read_dataframe = pyccd.read(spark_context, ids_df)
    assert type(read_dataframe) is spark_sql.dataframe.DataFrame
    assert set([i.asDict()["chipx"] for i in read_dataframe.collect()]) == set([100, 300])


def test_join(sql_context):
    df_attrs1 = ['chipx', 'chipy', 'pixelx', 'pixely', 'sday', 'eday', 'rfrawp']
    df_attrs2 = ['chipx', 'chipy', 'pixelx', 'pixely', 'sday', 'eday', 'srb3']
    ccd_df    = faux_dataframe(ctx=sql_context, attrs=df_attrs1)
    pred_df   = faux_dataframe(ctx=sql_context, attrs=df_attrs2)
    joined_df = pyccd.join(ccd=ccd_df, predictions=pred_df)
    assert set(['chipx', 'chipy', 'pixelx', 'pixely', 'sday', 'eday', 'srb3']) == set(joined_df.schema.names)

