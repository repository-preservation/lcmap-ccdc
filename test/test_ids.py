from firebird import ids

import json
import pyspark.sql.types

def test_schema():
    ids_schema = ids.schema()
    assert type(ids_schema) == pyspark.sql.types.StructType
    assert json.loads(ids_schema.json()) == {'fields': [{'metadata': {}, 'name': 'chipx', 'nullable': False, 'type': 'integer'}, 
                                                        {'metadata': {}, 'name': 'chipy', 'nullable': False, 'type': 'integer'}], 'type': 'struct'}

