import csv
from turtle import pd

import pandas

#read all csvs and store them into dataframes
df_con = pandas.read_csv ("input_data/cons.csv")
# print(df_con.columns)
df_con_email = pandas.read_csv ("input_data/cons_email.csv")
# print(df_con_email.columns)
df_con_email_ch_sub = pandas.read_csv ("input_data/cons_email_chapter_subscription.csv")
# print(df_con_email_ch_sub.columns)


#Produce a “people” file with the following schema. Save it as a CSV with a header line to the
# working directory. Schema- email, code. is_unsub, created_dt, updated_dt
# I am going to join the dataframes on common columns- cons_id and cons_email_id (cons_id from
# first 2 dataframes, and cons_email id from 2nd and 3rd dataframes)

df_combine_2 = pandas.merge(df_con, df_con_email, on="cons_id")
# print(df_combine_2.columns)

df_combine_3 = pandas.merge(df_combine_2, df_con_email_ch_sub, on="cons_email_id")
# print(df_combine_3.columns)

#now select only the specific columns we want from this final df
df_people = df_combine_3[["email", "source", "isunsub", "create_dt_x", "modified_dt_x"]]
print(df_people.columns)

#now rename columns to match the schema we want; renaming source to code because "code" description
# said it was the source; also renaming create_dt and modified_dt according to schema name in instructions
df_people_renamed = df_people.rename(columns={"source":"code", "create_dt_x": "created_dt", "modified_dt_x":"updated_dt"})
print(df_people_renamed.columns)

#write this as csv locally
df_people_renamed.to_csv('people.csv')

#new aggregate dataframe for acquisitions
df_people_aggregate = df_people_renamed.groupby('created_dt').created_dt.agg('count').reset_index(name='count')

#rename columns of aggregate
df_people_aggregate_rename = df_people_aggregate.rename(columns={"created_dt": "acquisition_date", "count":"acquisitions"})
print(df_people_aggregate_rename.columns)
print(df_people_aggregate_rename.info)

#write it as csv to local directory
df_people_aggregate_rename.to_csv('acquisition_facts.csv')