
# coding: utf-8

# In[1]:

get_ipython().magic(u'matplotlib inline')


# In[2]:

import findspark
import os
import sys
from matplotlib import pyplot as plt


# In[3]:

os.environ["SPARK_HOME"]="/spark"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.9-src.zip")


# In[4]:

findspark.init()


# In[5]:

from pyspark import SparkConf, SparkContext, SQLContext


# In[6]:

conf = SparkConf().set('spark.executor.instances',1).     set("spark.executor.memory", "5g").set("AppName", "testapp")


# In[7]:

sc = SparkContext(master="local", conf=conf)


# from pyspark.mllib.clustering import KMeans
# from numpy import array

# training_data = array([[1,1],[2,2],[1,3],[0.5,0],[0.3,3],[0.9,0.8],[0.9,1.2],[1.1,0.8],[1.8,1.5],[0.8,2.1],
#                  [3.1,3.3],[3.2,2.9],[3,5],[2.9,4.5],[0.3,0.4],[3,3],[4,4],[3.5,3.1],[3.9,4.2],[2.5,2.9]])

# dist_training_data = sc.parallelize(training_data)

# clusters = KMeans.train(dist_training_data, 2, maxIterations=10, runs=10, initializationMode="random")

# clusters.centers

# clusters.predict([1,4])

# clusters.predict([1,3])

# plt.scatter(*zip(*training_data), color='r', marker='*') 
# plt.scatter(*zip(*clusters.centers), color='g', marker='s')

# In[8]:

sqlcontext=SQLContext(sc)


# In[9]:

datafile = '/Users/Oldjun/Desktop/webapp/hospital/data/Mercy_HCAHPS_Survey_BXTN_04282016.dsv'


# df=sqlcontext.read.text(datafile)

# df.head(2)

# In[10]:

df = sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").option("mode", "DROPMALFORMED").option("delimiter","|").load(datafile)


# In[11]:

df.dtypes


# In[12]:

df1=df.filter(df["ANSWR_ID"]>0).filter(df["ST_CD"]=='MO').filter(df["QUES_ID"]<= 145)


# In[13]:

df1.head()


# In[14]:

df1.count()


# #create column age

# In[15]:

from pyspark.sql.functions import lit,substring


# In[16]:

df2=df1.withColumn("age",lit(0))


# In[17]:

df3=df2.withColumn("age",substring(df2["PTNT_AGE_QTY"],1,2))


# In[18]:

df2.head()


# In[19]:

df3.head()


# In[20]:

df4=df3.withColumn("age",df3.age.cast("int"))


# In[21]:

df4.head()


# In[22]:

sqlcontext.registerDataFrameAsTable(df4, "table4")


# In[23]:

df_data2=sqlcontext.sql("select               age,               BXNT_GENDER,               BXTN_RACE,               BXTN_RELIGION,               BXTN_LANGUAGE,               BXTN_FACILITY,               BXTN_LGTH_OF_STAY,               BXTN_PAYOR_GRP,               BXTN_INCOME_DESC,               BXTN_EDUCATION_LEVEL,               PRMRY_DX_CD,               PRMRY_PROC_ID,               DRG_MPI_CD,               BNFT_PLAN_ID,               QUES_ID,               ANSWR_ID               from table4")


# In[24]:

df_data2.head()


# In[25]:

df_data2.select(df_data2["ANSWR_ID"]).show()


# In[26]:

df_data2.describe("ANSWR_ID").show()


# In[27]:

df_data3=df_data2.groupBy('age','BXNT_GENDER',                'BXTN_RACE',               'BXTN_RELIGION',               'BXTN_LANGUAGE',               'BXTN_FACILITY',               'BXTN_LGTH_OF_STAY',               'BXTN_PAYOR_GRP',               'BXTN_INCOME_DESC',               'BXTN_EDUCATION_LEVEL',               'PRMRY_DX_CD',               'PRMRY_PROC_ID',               'DRG_MPI_CD',               'BNFT_PLAN_ID')               .pivot("QUES_ID").avg("ANSWR_ID")


# In[28]:

df_data2.show(5)


# df_data3.show(2)

# In[29]:

df_test=df_data2.groupBy('age','BXNT_GENDER',                'BXTN_RACE',               'BXTN_RELIGION',               'BXTN_LANGUAGE',               'BXTN_FACILITY',               'BXTN_LGTH_OF_STAY',               'BXTN_PAYOR_GRP',               'BXTN_INCOME_DESC',               'BXTN_EDUCATION_LEVEL',               'PRMRY_DX_CD',               'PRMRY_PROC_ID',               'DRG_MPI_CD',               'BNFT_PLAN_ID')               .pivot("QUES_ID").count()


# In[30]:

df_data3.columns


# In[31]:

df_data3.dtypes


# df_data3['16'].filter(df_data3['16']> 4) = NULL

# In[32]:

from pyspark.sql.functions import udf


# In[33]:

from pyspark.sql.types import StringType,DoubleType


# In[34]:

def addNA1(value,colname,dfspark):
    new_column_udf=udf(lambda name: None if name > value else name, DoubleType())
    return dfspark.withColumn(colname,new_column_udf(dfspark[colname]))


# In[35]:

add_col=[[4,'16'],[10,'33'],[6,'45'],[5,'145'],[5,'67'],[5,'70'],[5,'73']]


# In[36]:

test_new_df=df_data3


# In[37]:

for pair in add_col:
    value,colname=pair
    test_new_df=addNA1(value,colname,test_new_df)
    


# In[38]:

df_data3.where(df_data3["16"]>4).count()


# In[39]:

test_new_df.where(test_new_df['16'].isNull()).count()


# In[40]:

def addNA2(colname,dfspark):
    new_column_udf=udf(lambda name: None if name == 5 else name, DoubleType())
    return dfspark.withColumn(colname,new_column_udf(dfspark[colname]))


# In[41]:

addna_col=[17,18,19,20,21,23,25,26,28,29,34,144]
addna_col=map(lambda x:str(x),addna_col)


# In[42]:

for colname in addna_col:
    test_new_df=addNA2(colname,test_new_df)
    


# In[43]:

test_new_df.dtypes


# In[44]:

# mark all the questions, category them into topic
# teamwork, the lower the better


# In[45]:

df_teamwork=test_new_df.withColumn('teamwork',4.75 - test_new_df['45'] * 0.75 )


# In[46]:

from pyspark.sql.functions import col,avg,struct


# In[47]:

df_teamwork.dtypes


# In[48]:

df_test_avg = sqlcontext.createDataFrame(
    [(1, None, 23.0), (None, 2, -23.0),(2,3,4.0),(None,None,None)], ("x1", "x2", "x3"))


# In[49]:

df_test_avg.show()


# In[50]:

avg_row=udf(lambda row:            sum([x for x in row if x!=None])/(len([x for x in row if x!=None])) if len([x for x in row if x!=None])!=0                                               else None ,DoubleType())


# In[51]:

new_df = df_test_avg.withColumn("avg_row",avg_row(struct([df_test_avg[x] for x in df_test_avg.columns])))


# In[52]:

new_df.show()


# In[53]:

test_nurse=df_teamwork.withColumn("nurse",avg_row(struct([df_teamwork['13'],df_teamwork['14'],df_teamwork['15']])))


# test_nurse.show(2)

# In[54]:

df_doctor=test_nurse.withColumn('doctor',avg_row(struct([df_teamwork['17'],df_teamwork['18'],df_teamwork['19']])))


# In[55]:

df_enviroment=df_doctor.withColumn('environment',avg_row(struct([df_teamwork['20'],df_teamwork['21']])))


# In[56]:

df_pain=df_enviroment.withColumn('pain',avg_row(struct([df_teamwork['25'],df_teamwork['26']])))


# In[57]:

df_medication=df_pain.withColumn('medication',avg_row(struct([df_teamwork['28'],df_teamwork['29']])))


# In[58]:

df_discharge=df_medication.withColumn('discharge',7-3*avg_row(struct([df_teamwork['31'],df_teamwork['32']])))


# In[59]:

df_response=df_discharge.withColumn('response',avg_row(struct([df_teamwork['23'],df_teamwork['16']])))


# In[60]:

df_overall=df_response.withColumn('overall',                                  (df_teamwork['33']*0.4+df_teamwork['34']+ 4.75 - df_teamwork['73'] * 0.75)/3)


# In[61]:

df_overall.columns


# In[62]:

df_score=df_overall.withColumn('care',avg_row(struct([df_teamwork['143'],df_teamwork['145'],df_teamwork['144']])))


# In[63]:

df_respect=df_score.withColumn('respect',4.75 -  df_teamwork['67']*0.75)


# In[64]:

df_safety=df_respect.withColumn('safety',4.75 -  df_teamwork['70']*0.75)


# In[65]:

df_age2=df_safety.withColumn('age2',df_teamwork['age']**2)


# In[66]:

df_age2.columns


# In[67]:

len(df_age2.columns)


# In[68]:

colIdSelected = [x for x in range(10)]+[x for x in range(64,77)]


# In[69]:

colIdSelected


# In[70]:

colNameSelected = [df_age2.columns[x] for x in colIdSelected]


# In[71]:

colNameSelected


# In[72]:

df_age2.where(df_age2['medication'].isNull()).count()


# In[73]:

df_age2.count()


# In[74]:

df_data4=df_age2.na.drop(subset=colNameSelected)


# In[75]:

df_data4.count()


# In[76]:

len(df_data4.columns)


# In[77]:

df_age2.where(df_age2['pain'].isNull()).count()


# #convert string to factor

# In[78]:

from pyspark.ml.feature import StringIndexer


# In[79]:

df = sqlcontext.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["id", "category"])
print df.dtypes
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
indexed = indexer.fit(df).transform(df)
indexed.show()


# In[80]:

df_data4.dtypes


# In[81]:

df_data4.dtypes[0][1]


# In[82]:

colCat=[df_data4.dtypes[x][0] for x in range(len(df_data4.dtypes)) if df_data4.dtypes[x][1]=='string' ]


# In[83]:

colCat


# colCat.append('BNFT_PLAN_ID')

# colCat.append('DRG_MPI_CD')

# colCat

# In[84]:

df_data4.toPandas()['BXNT_GENDER'].unique()


# In[85]:

indexer = StringIndexer(inputCol='BXNT_GENDER', outputCol='BXNT_GENDERIndex')
indexed2 = indexer.fit(df_data4).transform(df_data4)


# In[86]:

for col in colCat:
    if col != "BXNT_GENDER":
        print col,col+"Index"
        colIndex=col+"Index"
        indexer = StringIndexer(inputCol=col, outputCol=colIndex)
        indexed2 = indexer.fit(indexed2).transform(indexed2)

    


# In[87]:

indexed2.first() 


# In[88]:

from pyspark.ml.regression import LinearRegression


# In[130]:

lr = LinearRegression(maxIter=10)#, regParam=0.3, elasticNetParam=0.8)


# In[90]:

from pyspark.mllib.regression import LabeledPoint


# In[109]:

training=indexed2.select("teamwork", "nurse","discharge")  .map(lambda r: LabeledPoint(r[0], r[1:]))


# In[110]:

training.cache()


# In[111]:

indexed2.cache()


# In[112]:

training.first()


# In[136]:

modelA = lr.fit(training.toDF(),{lr.regParam:0.0})


# In[137]:

from pyspark.mllib.regression import LinearRegressionWithSGD


# In[139]:

modelB=LinearRegressionWithSGD.train(training, iterations=10, step=0.1)


# In[125]:

pred=modelA.transform(training.toDF())


# In[126]:

pred.first()


# In[144]:

modelB.save(sc,"/Users/Oldjun/Desktop/webapp/hospital/sparkModel.test")


# In[127]:

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(metricName="rmse")
RMSE = evaluator.evaluate(pred)
print("ModelA: Root Mean Squared Error = " + str(RMSE))


# In[128]:

modelA.save(sc,"/Users/Oldjun/Desktop/webapp/hospital/sparkModel.test")


# In[ ]:



