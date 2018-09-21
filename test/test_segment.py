from ccdc import segment
from pyspark.sql import Row

import datetime
import json
import test


rows = [Row(cx=0,
            cy=1,
            px=3,
            py=4,
            sday="0",
            eday="1",
            bday="2",
            chprob=0.5,
            curqa=2,
            blmag=0.3,
            grmag=0.2,
            remag=0.55,
            nimag=0.22,
            s1mag=0.545,
            s2mag=0.1,
            thmag=0.04,
            blrmse=0.12,
            grrmse=0.22,
            rermse=0.44,
            nirmse=0.22,
            s1rmse=0.23,
            s2rmse=0.01,
            thrmse=0.77,
            blcoef=[0.1,0.2,0.3],
            grcoef=[0.1,0.2,0.3],
            recoef=[0.1,0.2,0.3],
            nicoef=[0.1,0.2,0.3],
            s1coef=[0.1,0.2,0.3],
            s2coef=[0.1,0.2,0.3],
            thcoef=[0.1,0.2,0.3],
            blint=1,
            grint=1,
            reint=1,
            niint=1,
            s1int=1,
            s2int=1,
            thint=1,
            rfrawp=[0.1,1.3,2.4,23.4,4.44],
            extra=True)]


def test_table():
    assert 'segment' == segment.table()


def test_schema():
    s = segment.schema().simpleString()
    assert s == 'struct<cx:int,cy:int,px:int,py:int,sday:string,eday:string,bday:string,chprob:float,curqa:int,blmag:float,grmag:float,remag:float,nimag:float,s1mag:float,s2mag:float,thmag:float,blrmse:float,grrmse:float,rermse:float,nirmse:float,s1rmse:float,s2rmse:float,thrmse:float,blcoef:array<float>,grcoef:array<float>,recoef:array<float>,nicoef:array<float>,s1coef:array<float>,s2coef:array<float>,thcoef:array<float>,blint:float,grint:float,reint:float,niint:float,s1int:float,s2int:float,thint:float,rfrawp:array<float>>'

    
def test_dataframe(spark_context, sql_context):    
    df   = sql_context.createDataFrame(rows)
    sdf  = segment.dataframe(spark_context, df).toJSON().collect()
    json = '{"cx":0,"cy":1,"px":3,"py":4,"sday":"0","eday":"1","bday":"2","chprob":0.5,"curqa":2,"blmag":0.3,"grmag":0.2,"remag":0.55,"nimag":0.22,"s1mag":0.545,"s2mag":0.1,"thmag":0.04,"blrmse":0.12,"grrmse":0.22,"rermse":0.44,"nirmse":0.22,"s1rmse":0.23,"s2rmse":0.01,"thrmse":0.77,"blcoef":[0.1,0.2,0.3],"grcoef":[0.1,0.2,0.3],"recoef":[0.1,0.2,0.3],"nicoef":[0.1,0.2,0.3],"s1coef":[0.1,0.2,0.3],"s2coef":[0.1,0.2,0.3],"thcoef":[0.1,0.2,0.3],"blint":1,"grint":1,"reint":1,"niint":1,"s1int":1,"s2int":1,"thint":1,"rfrawp":[0.1,1.3,2.4,23.4,4.44]}'
    
    print("SDF:{}".format(sdf))
    print("JSON:{}".format(json))
    assert sdf == [json]
    
    
def test_read_write(spark_context, sql_context):
    ids     = [Row(cx=0, cy=1)]
    idf     = sql_context.createDataFrame(ids)
    df      = sql_context.createDataFrame(rows)
    sdf     = segment.dataframe(spark_context, df)
    written = segment.write(spark_context, sdf).toJSON().collect()
    read    = segment.read(spark_context, idf).toJSON().collect()

    w = json.loads(written[0])
    r = json.loads(read[0])

    
    print("WRITTEN:{}".format(w))
    print(" ")
    print("READ:{}".format(r))
    assert r == w
