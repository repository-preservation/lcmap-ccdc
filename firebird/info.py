# point or bounds
# timeframe
# dataset
# operation - timeseries, changedetection, train, classify
#
# ===========================================
#
# Timeseries
# need chipids from bounds or from a point
# need to execute queries against chipmunk
# need to turn results into dataframe
#
# ===========================================
#
# ChangeDetection
# execute pyccd against inputs
# turn results into dataframe
# save dataframe to Cassandra point by point... account for version, acquired, input collection and version, hash results
#
# ===========================================
#
# Training
# Read changedetection results from Cassandra
# Get aux data as dataframe
# Format result as dataframe and save to Cassandra with tile id
#
# ===========================================
#
# Classification
# Classify tile by tile
# Snap point to tileid
# Read training from Cassandra by tile id
# Read whatever else
# Classify results and save to Cassandra point by point
#
