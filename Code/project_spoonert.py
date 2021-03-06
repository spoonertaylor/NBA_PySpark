# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 10:55:55 2017

Project
Taylor Spooner
spoonert
"""
# Need to open pyspark with 
# pyspark --packages com.databricks:spark-csv_2.10:1.3.0
# or spark-submit --packages com.databricks:spark-csv_2.10:1.3.0 file name

from pyspark import SparkContext
sc = SparkContext(appName=" SI_Project")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

import csv
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.ml.regression import LinearRegression

players = sqlContext.load(source="com.databricks.spark.csv", path = 'players_stats.csv', header = True,inferSchema = True)
players.registerTempTable("players")
#
## Load in data
shot_logs = sqlContext.load(source="com.databricks.spark.csv", path = 'shot_logs.csv', header = True,inferSchema = True)
shot_logs.registerTempTable("shots_1")

# Load in Teams
teams = sqlContext.load(source="com.databricks.spark.csv", path = 'NBA_Teams.csv', header = True,inferSchema = True)
teams.registerTempTable("teams_1")

#################
# Data manipulation
#################
# Bin the shots and save to a new dataframe
shots2 = sqlContext.sql("""select LOCATION, PERIOD, GAME_CLOCK, SHOT_CLOCK,
DRIBBLES, TOUCH_TIME, SHOT_DIST, PTS_TYPE, SHOT_RESULT, CLOSEST_DEFENDER, 
CLOSEST_DEFENDER_PLAYER_ID, CLOSE_DEF_DIST, FGM as MADE, PTS as PTS_SHOT,
player_name, player_id,  
	case when PTS_TYPE = 3 then "Three"
	when PTS_TYPE = 2 and SHOT_DIST <= 8 then "Paint"
	else "Mid_Range" end as SHOT_BUCKET,
    case when DRIBBLES > 0 then 1
    else 0 end as SHOT_TYPE
from shots_1""")
shots2.registerTempTable("shots")

# Bin the offensive ratings.
teams2 = sqlContext.sql("""select TEAM, GP, W, L, OFFRTG, DEFRTG,
                        NETRTG,ASTPer, AST_TO, AST_RAT, OREBPer,
                        DREBPer, REBPer, TOVPer, EFGPer, TSPer,
                        PACE, PIE, TEAM_ABBV,
	case when OFFRTG < 96 then "[93, 96)"
	when OFFRTG < 99 then "[96, 99)"
	when OFFRTG < 102 then "[99, 102)"
	when OFFRTG < 105 then "[102, 105)"
	else "[105, 111)" end as OFF_BINS
from teams_1""")
teams2.registerTempTable("teams")

######################
# Join tables
######################
joined = sqlContext.sql("""select * from (
	select * from shots s
		left join players p
		on s.player_name = lower(p.Name)) t1
left join teams t
on t1.Team = t.TEAM_ABBV""")
joined.registerTempTable("nba")
######################
# Task 1: Logistic Regression, 
# shot outcome ~ distance + def_dis + touch_time + dribbles + FG% of player
######################
def get_shot_stats(line):
    shot_outcome = line['FGM']
    dist = line['SHOT_DIST']
    def_dist = line['CLOSE_DEF_DIST']
    shot_type = bool(line['SHOT_TYPE'])
    pts = line[18]
    if pts == 3:
        fg = line['3P%']
    else:
        fg = line['FG%']
    if shot_outcome:
        shot = 1
    else:
        shot = 0
    return (shot, (dist, def_dist, shot_type,fg))

q1 = joined.rdd
q1b = q1.map(get_shot_stats).filter(lambda x: x[0] is not None and
            x[1][0] is not None and x[1][1] is not None and
             x[1][2] is not None and x[1][3] is not None)
q1c = q1b.map(lambda x: LabeledPoint(x[0], (x[1][0], x[1][1],x[1][2],x[1][3])))
train, test = q1c.randomSplit([0.8, 0.2], seed=12345)
# Run the logistic regression
logit_model = LogisticRegressionWithLBFGS.train(train)
# Find the test accuracy
labels_and_preds = test.map(lambda p: (p.label, logit_model.predict(p.features)))
test_accuracy = labels_and_preds.filter(lambda (v, p): v == p).count() / float(test.count())

q1b.collect()
q1b.map(lambda i: ','.join(str(j) for j in i))
with open('output_shots.csv', 'wb') as csvfile:
    f = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    f.writerow(["SHOT_OUTCOME", "FEATURES"])
    for row in q1b.collect():
        f.writerow(row)
#######################
# Task 2: Do players get better with experience?
#######################
def get_player_scoring(line):
    player_id = line['player_id']
    exp = line['Experience']
    if exp is None:
        exp = -1
    elif exp == 'R':
        exp = 0
    elif not (exp.isdigit()):
        exp = -1
    pts = line[26]
    minutes = line['MIN']
    return ((int(player_id), int(exp)), (pts, minutes))


# Group older: 
def group_olders(line):
    exp = line[0]
    if exp >= 14:
        exp = "14+"
    return (exp, (line[1][0], line[1][1]))

q2 = joined.rdd
# Get scoring per 48 minutes
q2b = q2.map(get_player_scoring).filter(lambda x: x[1][1] >= 250).filter(lambda x: x[0][1] >= 0)
# Reduce by experience

q2c = q2b.mapValues(lambda x: float(x[0])/x[1]*48).mapValues(lambda x: (x, 1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
q2d = q2c.mapValues(lambda x: float(x[0])/x[1])
q2e = q2d.map(lambda x: (x[0][1], (x[1],1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
q2f = q2e.map(group_olders).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
q2g = q2f.mapValues(lambda x: float(x[0])/x[1])

lin_reg = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8,
                           featuresCol = 'features', labelCol = 'label')

lr_data = q2d.map(lambda x: (x[0][1], x[1])).map(lambda x: LabeledPoint(x[1], [x[0], x[0]*x[0]])).toDF()

lrModel = lin_reg.fit(lr_data)

# Print the coefficients and intercept for linear regression
print("Coefficients: " + str(lrModel.coefficients)) # 0.0299
print("Intercept: " + str(lrModel.intercept)) # 19.43

## Get predictions
#valuesAndPreds = lr_data.map(lambda p: float(lrModel.predict(p.features)))#, p.label))
#
## Instantiate metrics object
#metrics = RegressionMetrics(valuesAndPreds)
#
## Squared Error
#print("MSE = %s" % metrics.meanSquaredError)
#print("RMSE = %s" % metrics.rootMeanSquaredError)
#
## R-squared
#print("R-squared = %s" % metrics.r2)

q2g.collect()
q2g.map(lambda i: ','.join(str(j) for j in i))
with open('output_experience2.csv', 'wb') as csvfile:
    team_props = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    team_props.writerow(["EXPERIENCE", "POINTS"])
    for row in q2g.collect():
        team_props.writerow(row)

########################
# Task 3: What do positions looks like?
#######################
q3 = players.rdd


def get_player_stats(line):
    mins = line['MIN']
    pts = line['PTS']
    fga = line['FGA']
    fgp = line['FG%']
    thr_a = line['3PA']
    thr_p = line['3P%']
    fta = line['FTA']
    ftp = line['FT%']
    oreb = line['OREB']
    dreb = line['DREB']
    ast = line['AST']
    stl = line['STL']
    blk = line['BLK']
    tov = line['TOV']
    
    return (mins,pts, fga,fgp,thr_a,thr_p,fta,ftp,oreb,dreb,ast,stl,blk,tov)

q3a = q3.map(get_player_stats).filter(lambda x: x[0] is not None \
            and x[1] is not None and x[2] is not None and x[3] is not None \
                 and x[4] is not None and x[5] is not None and x[6] is not None \
                      and x[7] is not None and x[8] is not None and x[9] is not None \
                           and x[10] is not None and x[11] is not None and x[12] is not None\
                                and x[13] is not None)\
            .filter(lambda x: x[0] >= 1000)
q3a_2 = q3a.map(lambda x: (float(x[1])/x[0]*48, x[2], float(x[3])/x[0]*48,
                                 x[4], float(x[5])/x[0]*48, float(x[6])/x[0]*48,
                                  float(x[7])/x[0]*48, float(x[8])/x[0]*48,
                                       float(x[9])/x[0]*48, float(x[10])/x[0]*48,
                                            float(x[11])/x[0]*48, float(x[12])/x[0]*48, 
                                                 float(x[13])/x[0]*48))

# Build the model (cluster the data)
clusters = KMeans.train(q3a_2, 8, maxIterations=50,
        runs=50, initializationMode="random")

# Here are all the clusters
q3b = q3a_2.map(lambda point: clusters.predict(point))
# Add this to players

q3c = q3.filter(lambda x: x['MIN'] is not None).filter(lambda x: x['MIN'] >= 1000).zip(q3b)

# It will be interesting to see who is in each cluster
cluster_0 = q3c.filter(lambda x: x[1] == 0).map(lambda x: x[0][0]) 
cluster_1 = q3c.filter(lambda x: x[1] == 1).map(lambda x: x[0][0])
cluster_2 = q3c.filter(lambda x: x[1] == 2).map(lambda x: x[0][0])
cluster_3 = q3c.filter(lambda x: x[1] == 3).map(lambda x: x[0][0])
cluster_4 = q3c.filter(lambda x: x[1] == 4).map(lambda x: x[0][0])
cluster_5 = q3c.filter(lambda x: x[1] == 5).map(lambda x: x[0][0]) 
cluster_6 = q3c.filter(lambda x: x[1] == 6).map(lambda x: x[0][0]) 
cluster_7 = q3c.filter(lambda x: x[1] == 7).map(lambda x: x[0][0]) 

cluster_0.take(20)
cluster_1.take(20)
cluster_2.take(20)
cluster_3.take(20)
cluster_4.take(20)
cluster_5.take(20)
cluster_6.take(20)
cluster_7.take(20)


# Now get average person data
def get_player_stats(line):
    clust = line[1]
    height = line[0]['Height']
    weight = line[0]['Weight']
    age = line[0]['Age']
    
    return (int(clust), (height, weight, age, 1))

q3d = q3c.map(get_player_stats).filter(lambda x: x[1][0] is not None)
# Get totals for each cluster.
q3e = q3d.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3]))
# Averages for each cluster.
q3f = q3e.mapValues(lambda x: (float(x[0])/x[3], float(x[1])/x[3], float(x[2])/x[3]))
# Get data in format to write to csv.
q3g = q3f.map(lambda x: (x[0], x[1][0], x[1][1],x[1][2]))
q3g.collect()
q3g.map(lambda i: ','.join(str(j) for j in i))

# Save to csv file
with open('output_personal_info.csv', 'wb') as csvfile:
    f = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    f.writerow(["Cluster", "Height", "Weight", "Age"])
    for row in q3g.collect():
        f.writerow(row)


phys_attr = sqlContext.sql("""select max(Height) as Max_Height, min(Height) as Min_Height, avg(Height) as Avg_Height, stddev(Height) as sd_Height,
	max(Weight) as Max_Weight, min(Weight) as Min_Weight, avg(Weight) as Avg_Weight, stddev(Weight) as sd_Weight,
	max(Age) as Max_Age, min(Age) as Min_Age, avg(Age) as Avg_Age, stddev(Age) as sd_Age
	from nba""")

######################
# Task 4: Look at proportion of shot types per offensive rating
######################

# We are just going to do this one in SQL
props = sqlContext.sql("""select t1.OFF_BINS, t1.SHOT_BUCKET, t1.cnt / t2.tot_cnt as prop from (
	select OFF_BINS, SHOT_BUCKET, count(*) as cnt
		from nba
		group by OFF_BINS, SHOT_BUCKET) t1
	join (
		select OFF_BINS, count(*) as tot_cnt from nba group by OFF_BINS) t2
	on t1.OFF_BINS = t2.OFF_BINS""")

ex_points = joined.rdd

# Now get average person data
def get_shot_points(line):
    shot = line['SHOT_BUCKET']
    pts = line['PTS_SHOT']    
    return (shot, (pts, 1))

ex_points = ex_points.map(get_shot_points)\
                         .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
                                     .mapValues(lambda x: float(x[0])/x[1])
# Expected Points
# Paint 1.13
# Mid range 0.802
# Three 1.06

props.collect()
props.rdd.map(lambda i: ','.join(str(j) for j in i))
with open('output_team_props.csv', 'wb') as csvfile:
    team_props = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    team_props.writerow(["OFF_RAT", "SHOT_TYPE", "Prop"])
    for row in props.collect():
        team_props.writerow(row)