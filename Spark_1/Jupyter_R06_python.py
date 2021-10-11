#!/usr/bin/env python
# coding: utf-8

# ### Initialize a sparkSession

# In[1]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import  SQLContext
from pyspark.sql import SparkSession
from pyspark import  SQLContext
from pyspark import SparkConf, SparkContext

spark_1 = SparkSession.builder.appName('Historical').getOrCreate()


# ### Load the Historical data

# In[17]:


df = spark_1.read.csv('hdfs:///SP_500_Historical.csv', inferSchema = True, header = True)


# ### Show the column names

# In[18]:


df.columns


# ### Display the first row of the data

# In[19]:


df.head()


# ### Check the schema of the dataframe 

# In[20]:


df.printSchema()


# ### Print the first 5 rows

# In[22]:


for line in df.head(5):
    print(line, '\n')


# ### Print Statistics of the data

# In[23]:


df.describe().show()


# ### Format columns to show 2 decimal places

# In[25]:


from pyspark.sql.functions import format_number

summary = df.describe()
summary.select(summary['summary'],
                   format_number(summary['Open'].cast('float'), 2).alias('Open'),
                   format_number(summary['High'].cast('float'), 2).alias('High'),
                   format_number(summary['Low'].cast('float'), 2).alias('Low'),
                   format_number(summary['Close'].cast('float'), 2).alias('Close'),
                   format_number(summary['Volume'].cast('int'), 0).alias('Volume')).show()


# ### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day

# In[27]:


df_hv = df.withColumn('HV Ratio', df['High']/df['Volume']).select(['HV Ratio'])
df_hv.show()


# ### Which day has the peak high in price?

# In[28]:


df.orderBy(df['High'].desc()).select(['Date']).head(1)[0]['Date']


# ### What is the mean of the "Close" column

# In[29]:


from pyspark.sql.functions import mean
df.select(mean('close')).show()


# ### What is the maximum and minimum value of the "Volume" column

# In[30]:


from pyspark.sql.functions import min, max
df.select(max('Volume'), min('Volume')).show()


# ### How many days did the stocks close lower than 60 dollars?

# In[31]:


df.filter(df['Close'] < 60).count()


# ### What percentage of the time was the "High" greater than 80 dollars? 

# In[32]:


df.filter('High > 80').count() * 100/df.count()


# ### What is the Pearson correlation between "High" and "Volume"

# In[33]:


df.corr('High', 'Volume')


# In[34]:


from pyspark.sql.functions import corr
df.select(corr(df['High'], df['Volume'])).show()


# ### What is the max "High" per year? 

# In[37]:


from pyspark.sql.functions import (dayofmonth, hour, dayofyear, month, year, weekofyear, format_number, date_format)
year_df = df.withColumn('Year', year(df['Date']))
year_df.groupBy('Year').max()['Year', 'max(High)'].show()


# ### What is the average "Close" for each calender month

# In[38]:


#Create a new column month from existing date column
month_df = df.withColumn('Month', month(df['Date']))

#Group by month and take average of all other columns
month_df = month_df.groupBy('Month').mean()

#Sort by month
month_df = month_df.orderBy('Month')

#Display only month and avg(Close), the desired columns
month_df['Month', 'avg(Close)'].show()

