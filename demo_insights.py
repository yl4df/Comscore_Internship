# -*- coding: utf-8 -*-
"""
This python file combines three sql files and is used to write out data from greenplum based on our requirement. 
"""

import subprocess
import sys
from dateutil import relativedelta as rdelta
import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from os.path import basename
import importlib

 


'''install packages that may not exist in some python distributions'''
def install(packages):
    for package in packages:
        try:
            importlib.import_module(package)
        except:
            subprocess.call([sys.executable, "-m", "pip", "install", package])
            
            
            
            
            
'''
Method used to send attachments to email list from dummy email
'''
def send_mail(send_from: str, subject: str, text: str, 
send_to: list, files= None, password = None):

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(send_to)  
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil: 
            ext = f.split('.')[-1:]
            attachedfile = MIMEApplication(fil.read(), _subtype = ext)
            attachedfile.add_header(
                'content-disposition', 'attachment', filename=basename(f) )
        msg.attach(attachedfile)


    smtp = smtplib.SMTP(host="smtp.gmail.com", port= 587) 
    smtp.starttls()
    smtp.login(send_from,password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()
    
    
    
    
    
'''
The method is a helper method to calculate the upper and lower bounds: 12-month rolling mean +/- 12 rolling standard deviation;
The method works on the grouped dataframe and returns the bollinger bands along with the original data for each group
'''
def get_bollinger(x):
    boll_cols = ['uv_pct', 'page_views_pct', 'duration_pct']
    x.set_index('month_id')
    for col in boll_cols:
        ma = x[col].rolling(window = 12).mean()
        std = x[col].rolling(window = 12).std()
        ma.iloc[-1] = ma.iloc[-2]
        std.iloc[-1] = std.iloc[-2]
        #the above two statements set the most recent month to the previous month's values, since the bollinger deviation
        #for time T is taken with respect to the upper and lower bounds at T - 1
        x['upper_' + col] = ma + (std * 2)
        x['lower_' + col] = ma - (std * 2)
        #above can be changed for desired window and deviation threshold
    return x





'''
The method is a helper method to query greenplum by given connection and return a dataframe
'''
def get_query(query_string, con):
    return pd.read_sql_query(query_string, con)





'''
The method is to filter top 10 enetities for each category ranked by unique visitors for the most recent month 
The method takes a dataframe and returns np.array of web_id; This is a helper method that will be used in the below method
To avoid multiple entities under one hierarchy_id appearing, we only select the entity with lowest depth for each group of cat_subcat_id and hierarchy_id
'''
def find_top(full_joined):
    full_joined = full_joined.groupby(['cat_subcat_id', 'hierarchy_id']).apply(lambda x: x.loc[x['depth'].idxmin(axis = 1)])
    full_joined.index = full_joined.index.droplevel(1)
    full_joined = full_joined.drop(labels = 'cat_subcat_id', axis = 1).reset_index().groupby('cat_subcat_id').apply(lambda x: x.loc[x['visitors_proj'].nlargest(10).index]) 
    #the line above can be changed to take the desired nlargest entities under each category
    full_joined.index = full_joined.index.droplevel(0)
    return full_joined.web_id.unique()
    





'''
The method is to generate the whole list of desired web_ids and full category ids that we will use later, including top 10 entities for each category 
Also returns a data frame with the category name/id for each web id, and the same for the full categories, to be used for a later join
'''
def get_web_id_set(conn, start_month, population_id):
    metrics_query = '''select web_id, visitors_proj from comscore.mpmmx_web_agg_{}m_50000
    where location_id = 100 and population_id = {}'''.format(str(start_month), population_id)
    cat_subcat_filter_query = 'select cat_subcat_id, cat_subcat_name from comscore.mm200_cat_subcat_lookup where parent_id = 1' 
    cat_subcat_filter_df = get_query(cat_subcat_filter_query, con = conn)
    full_category_ids = np.unique(cat_subcat_filter_df['cat_subcat_id'])
    cat_subcat_query = """select * from comscore.mm200_cat_subcat_map_{}m 
    where cat_subcat_id in {}""".format(str(start_month), tuple(full_category_ids))
    cat_map = cat_subcat_filter_df.merge(right = get_query(cat_subcat_query, con = conn), how = 'inner', left_on = 'cat_subcat_id', right_on = 'cat_subcat_id')
    join_query = '''select web_id, hierarchy_id, depth from comScore.mm200_hierarchy_web_lookup_{}m'''.format(start_month)
    full_joined_names = find_top(get_query(join_query, con = conn).merge(right = pd.merge(cat_map, get_query(metrics_query, con = conn), how = 'inner', left_on = 'web_id', right_on = 'web_id'), how = 'inner', left_on = 'web_id', right_on = 'web_id'))
    cols = ['cat_subcat_id','cat_subcat_name', 'web_id']
    web_id_set = np.union1d(full_joined_names, full_category_ids)
    names = pd.DataFrame(web_id_set, columns = ['web_id'])
    return tuple(web_id_set), pd.concat([cat_map.merge(names, how = 'inner', left_on = 'web_id', right_on = 'web_id')[cols], cat_subcat_filter_df.merge(names, how = 'inner', left_on = 'cat_subcat_id', right_on = 'web_id')[cols]])





'''
The method is to re-group gender age id, because the old version of gender age id is too detailed to provide insights;
The method takes a list of gender_age id and returns a dictionary mapping old version to the new version; this is a helper method. 
'''
def get_gender_dict(gender_labels):
    dict_for_clean = {}
    for string in gender_labels:
        split = [word for word in string.split(':')]
        second = split[-1].strip().split('-')
        if '+' in second[-1]:
            dict_for_clean[string] = split[0] + ': 55+' 
        elif int(second[-1]) <= 17:
            dict_for_clean[string] = split[0] + ': 2-17'
        elif int(second[-1]) <= 24:
            dict_for_clean[string] = split[0] + ': 18-24'
        elif int(second[-1]) <= 34:
            dict_for_clean[string] = split[0] + ': 25-34'
        else:
            dict_for_clean[string] = split[0] + ': 35-54'
    return dict_for_clean





'''Method limits the return results to 245 records to match with vba, can be changed in the future in tandem with the vba code'''
def get_245(df):
    month = df['month_id'].max()
    not_cat = df[df['web_name'] != df['cat_subcat_name']]
    cat = df[df['web_name'] == df['cat_subcat_name']]
    k = df[df['web_id'].isin(set(not_cat[not_cat['month_id'] == month].groupby('web_id').sum()['uv'].sort_values(ascending = False).index[:245 - len(cat.web_name.unique())]))]
    return pd.concat([k,cat])
    #setting a strict limit on the number of rows pulled to simplify vba process, this can be changed in tandem with the respective vba code





'''
The method is to append labels for demographic ids by merging the dataframe with according demographic dictionary table;
Instead of looking at the raw number, we want to see the composition percentage for each group of month_id and web_id. 
The following is to add new columns for percentage and apply get_bollinger to get upper and lower bounds.
'''
def refine_df(bucket_time_series_df, cat_subcat_join_df, bucket, population_id, conn):

    if bucket == 'hh_income_id':
        join_query = """select value_label, demo_value 
        from comScore.mm200_demographics_lookup where demo_value in(84007, 84006, 84005, 84011, 84003, 84004) and demo_name = '{}'""".format(bucket)
        bucket_time_series_df = bucket_time_series_df.merge(right = get_query(join_query, con = conn), how = 'inner', left_on = 'hh_income_id', right_on = 'demo_value')
        bucket_time_series_df = bucket_time_series_df.merge(right = cat_subcat_join_df, how = 'inner', left_on = 'web_id', right_on = 'web_id')
        bucket_time_series_df['web_name'] = bucket_time_series_df['web_name'].fillna(bucket_time_series_df['cat_subcat_name'])
        bucket_time_series_df = bucket_time_series_df.drop(labels = 'demo_value', axis = 1)
        
       
    elif bucket == 'gender_age_id':
        join_query = '''select distinct value, desc_text, gender_id 
        from comScore.mm200_gender_age_lookup where population_id = {}'''.format(population_id)
        bucket_time_series_df = bucket_time_series_df.merge(right = get_query(join_query, con = conn), how = 'inner', left_on = 'gender_age_id', right_on = 'value')
        bucket_time_series_df = bucket_time_series_df.merge(right = cat_subcat_join_df, how = 'inner', left_on = 'web_id', right_on = 'web_id')
        bucket_time_series_df['web_name'] = bucket_time_series_df['web_name'].fillna(bucket_time_series_df['cat_subcat_name'])
        bucket_time_series_df['gender_id'] = np.where(bucket_time_series_df['gender_id'] == 1, 'Male', 'Female') 
        bucket_time_series_df['gender_age_id'] = bucket_time_series_df['desc_text'].replace(to_replace = get_gender_dict(np.unique(bucket_time_series_df['desc_text'])))
        bucket_time_series_df = bucket_time_series_df.drop(labels = ['value', 'desc_text'], axis = 1)
        bucket_time_series_df = bucket_time_series_df.groupby(['web_id', 'month_id', 'gender_age_id', 'cat_subcat_name', 'cat_subcat_id', 'gender_id', 'web_name']).sum().reset_index()
        
    
    else:
        bucket_time_series_df = bucket_time_series_df.merge(right = cat_subcat_join_df, how = 'inner', left_on = 'web_id', right_on = 'web_id')
        bucket_time_series_df['web_name'] = bucket_time_series_df['web_name'].fillna(bucket_time_series_df['cat_subcat_name'])
    
    
    bucket_time_series_df[['page_views_pct', 'duration_pct', 'uv_pct']] = bucket_time_series_df.groupby(['web_id', 'month_id']).apply(lambda x: 100 * x[['page_views', 'duration', 'uv']]/x[['page_views', 'duration', 'uv']].sum(axis = 0))
    #above is the pct breakdown step
    bucket_time_series_df = bucket_time_series_df.groupby(['web_id', bucket]).apply(get_bollinger)
    #for retreiving bollinger bands

    cols = list(bucket_time_series_df.columns)
    cols = [cols[0], cols[6], cols[2], cols[1]] + [cols[i] for i in range(3, len(cols)) if i != 6]
    
    #temp1 = bucket_time_series_df[~(bucket_time_series_df['web_name'] != bucket_time_series_df['cat_subcat_name'])]
    #temp = bucket_time_series_df[bucket_time_series_df['web_name'] != bucket_time_series_df['cat_subcat_name']]
    #k = bucket_time_series_df[bucket_time_series_df['web_id'].isin(set(temp[temp['month_id'] == temp.month_id.max()].groupby('web_id').sum()['uv'].sort_values(ascending = False).index[:216]))]
    
    return get_245(bucket_time_series_df[cols])




    
'''
The method is to loop through each month and generate the time_series data for each month_id and web_id
'''
def generate_time_series(conn, bucket, end_month, start_month, web_id_set, population_id, cat_subcat_join_df):
    df_list = []
    hierarchy_join_query = '''select web_id, web_name from comScore.mm200_hierarchy_web_lookup_{}m where web_id in {}'''.format(start_month, web_id_set)
 
    for month in range(end_month, 231):
    #change above to end_month, start_month + 1
        ltt_table = 'comscore.Mpmmx_ltt_{}m_50000'.format(str(month))
        #the above table can be changed for mobile, desktop, or any table that contains the columns required for the query below (including columns for gender_age_id, hh_income_id, and children_id)
        #the function itself can be changed to provide a new param for the table name, making the process applicable to all tables and all buckets (make sure to change the write to directory method to include this new param also)
        temp_query = '''select web_id, month_id, {}, sum(visitors_proj) as UV, sum(pages_proj) as page_views, 
                    sum(minutes_proj) as duration from {} where web_id in {} and population_id = {} and location_id = 100 group by web_id, month_id, {}'''.format(bucket, ltt_table, web_id_set, population_id, bucket)
        temp_df = get_query(temp_query, con = conn)
        df_list.append(temp_df)
       
    bucket_time_series_df = pd.concat(df_list)
    #Make sure no data is missing. 
    bucket_time_series_df = bucket_time_series_df[bucket_time_series_df.groupby(['web_id', bucket]).uv.transform(len) == 231 - end_month].merge(right = get_query(hierarchy_join_query, con = conn), how = 'left', left_on = 'web_id', right_on = 'web_id')
    #change above equality to start_month - end_month + 1 (in the transform equality)
    return refine_df(bucket_time_series_df, cat_subcat_join_df, bucket, population_id, conn)





'''Writes files to the shared directory to be grabbed by excel workbook'''
def write_to_directory(bucket_time_series_df, bucket):
    file_path = '\\\\CSIADDFS01\SyndicatedOps\MoMX\yunlu&alex\Final_txt\For_Retrieval\{}5.txt'.format(bucket)
    if os.path.isfile(file_path):
        os.remove(file_path)
        bucket_time_series_df.to_csv(path_or_buf = file_path, sep = '\t')
    else:
        bucket_time_series_df.to_csv(path_or_buf = file_path, sep = '\t')




    
'''starting the process'''    
def get_bucket_time_series(bucket_list, conn, end_month, start_month, population_id):
    web_id_set, cat_subcat_join_df = get_web_id_set(conn, start_month, population_id)
    for bucket in bucket_list:
        write_to_directory(generate_time_series(conn, bucket, end_month, start_month, web_id_set, population_id, cat_subcat_join_df), bucket)
     




                    
'''to find pct reach in final report'''
def write_out_total_internet_uv(month,con, population_id):
    total_internet_df = get_query('''select visitors_proj from comscore.mpmmx_web_agg_{}m WHERE population_id={} and web_id = 1 and location_id = 100'''.format(month, population_id),con)
    file_path = '\\\\CSIADDFS01\SyndicatedOps\MoMX\yunlu&alex\Final_txt\For_Retrieval\\total_internet_uv.txt'
    if os.path.isfile(file_path):
        os.remove(file_path)
        total_internet_df.to_csv(path_or_buf = file_path, sep = '\t')
    else:
        total_internet_df.to_csv(path_or_buf = file_path, sep = '\t')





if __name__ == "__main__":
    install(['psycopg2', 'pandas', 'numpy','psycopg2.extras'])
    import psycopg2
    import psycopg2.extras
    import pandas as pd
    import numpy as np
    d1 = datetime.datetime.now()
    d2 = datetime.datetime(1999,12,31)
    rd = rdelta.relativedelta(d1,d2)
    start_month = rd.years * 12 + rd.months - 1
    end_month = 202
    #can be altered in the future 
    population_id = 840
    bucket_list = ['hh_income_id', 'gender_age_id', 'children_id']
    conn_string = "host='csia2gpm01' dbname='gp_edw' user='{}' password='{}'".format('yuli', 'LYL980324!')
    #change the .format above to the desired username and password for future runs
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor = conn.cursor()
    get_bucket_time_series(bucket_list, conn, end_month, start_month, population_id)
    write_out_total_internet_uv(230,conn, population_id)
    #To get the most recent month, please change 230 to start_month(already defined above)
    conn.close()
    
    send_list = ['yuli@comscore.com', 'asnow@comscore.com']
    username = 'monthly.sends@gmail.com'
    password = '!sTrB1Tg'
    send_mail(send_from = username, 
              send_to = send_list, 
              subject = 'Demographic Trend Report', 
              text = "Here's this month's demographic anamolies", 
              files = ['\\\\CSIADDFS01\\SyndicatedOps\\MoMX\\yunlu&alex\\excel_example\\macro_enabled\\final_ranking_report_macro.xlsm'],
              password = password)
    #send_to list can be modified above, as well as files to send (currently sending finalized excel files)
