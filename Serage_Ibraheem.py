#!/usr/bin/env python
# coding: utf-8

# In[32]:


import pandas as pd
import os
import csv
import boto3
import io


# # A. Objective: Create a single CSV file containing the following 5 fields

# In[33]:


# Declare csv fields
columns = ["patient_id", "enrollment_start_date", "enrollment_end_date", "ct_outpatient_visits", "ct_days_with_outpatient_visit"]


# In[34]:


# assign name for the csv file
csv_data = "result.csv"


# In[ ]:


# Creating csv file
def csv_creator():
    with open(file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(columns)


# In[35]:


# to make sure we have unique values of the three columns; create the following function
def drop_dups(df):
    final = results.drop_duplicates(subset=["patient_id", "enrollment_start_date", "enrollment_end_date"])
    
    return final


# # B. Prompt: To achieve the objective, follow the steps below and provide the required information in your answer sheet.

# In[36]:


# Create a function to connect and read  the file
def read_aws_file(bucket_name, object_key, aws_access_key_id, aws_secret_access_key):

    # initiat with aws
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Create session 
    s3 = session.client('s3')

   
    s3_file_location = {
        'Bucket': bucket_name,
        'Key': object_key
    }

    # retrieve the file
    try:
        response = s3.get_object(**s3_file_location)
        file_content = response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error: {e}")
        return None

    # Saving it to Pandas DataFrame
    df = pd.read_csv(io.StringIO(file_content))

    return df


# In[37]:


# Run and download the csv file from S3 bucket
df = read_aws_file('waymark-assignment', 'patient_id_month_year.csv', 'AKIAZLXG4RYJBLE4OTXT', 'bWGKTChCrTEJU1mP93e6zCYDO49XAkTrtGP7VoAc')


# In[38]:


# Copy the data to wd (working data)
wd = df.copy()


# In[39]:


# Define new columns names
cols_names = ['patient_id', 'enrollment_start_date', 'enrollment_end_date']


# In[40]:


# rename column names to patient_id x enrollment_start_date x enrollment_end_date
wd.set_axis(cols_names, axis='columns', inplace=True)


# In[41]:


wd.head()


# In[42]:


# Drop empty records
wd = wd.dropna(subset=['patient_id'])


# In[43]:


# report the number of rows in this file
print(f"Number of rows in patient_enrollment_span is {len(wd['patient_id'])}")


# In[44]:


# saving the results to csv file
wd.to_csv("patient_enrollment_span.csv", index=False)


# # Step 2: Data Aggregation

# In[45]:


# Run and download the csv file from S3 bucket
df_2 = read_aws_file('waymark-assignment', 'outpatient_visits_file.csv', 'AKIAZLXG4RYJBLE4OTXT', 'bWGKTChCrTEJU1mP93e6zCYDO49XAkTrtGP7VoAc')


# In[46]:


wd_2 = df_2[['patient_id', 'date', 'outpatient_visit_count']].copy()


# In[47]:


wd_2.head()


# In[48]:


# Drop empty records
wd_2 = wd_2.dropna(subset=['patient_id'])


# In[49]:


# Count of valid records
len(wd_2['patient_id'])


# In[50]:


# Clean any spaces that the patient_id might have
wd['patient_id'] = wd['patient_id'].str.strip()
wd_2['patient_id'] = wd_2['patient_id'].str.strip()


# In[51]:


# Merge the two dataframes to create result.csv
merged_df = wd.merge(wd_2, left_on='patient_id', right_on='patient_id', how='inner') # using the inner method to avoid lossing any data


# In[52]:


merged_df.head()


# In[53]:


# Creating ct_outpatient_visits
merged_df['ct_outpatient_visits'] = merged_df.groupby('patient_id')['outpatient_visit_count'].transform('sum')


# In[54]:


# Converting date to datetime
merged_df['date'] = pd.to_datetime(merged_df['date'])


# In[55]:


# Creating ct_days_with_outpatient_visit
merged_df['ct_days_with_outpatient_visit'] = merged_df.groupby('patient_id')['date'].transform('nunique')


# In[56]:


# Creating the final data frame
results = merged_df[['patient_id', 'enrollment_start_date', 'enrollment_end_date', 'ct_outpatient_visits', 'ct_days_with_outpatient_visit']].copy()


# In[57]:


results.head()


# In[58]:


print(f"Number of distinct values of ct_days_with_outpatient_visit in result.csv is {len(results['ct_days_with_outpatient_visit'].unique())}")


# In[59]:


# Make sure of unique entries
final = drop_dups(results)


# In[60]:


# Save the final dataframe to results.csv
final.to_csv("result.csv", index=False)


# In[ ]:





# In[ ]:




