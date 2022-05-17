import time
from datetime import datetime, timedelta
import ciso8601
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pmaw import PushshiftAPI 

# https://github.com/asad70/reddit-sentiment-analysis

api = PushshiftAPI()

# ini_date refers to the first date to be scraped from going back in time. end_date is the last date to be scraped.
# TODO either change the loop to scrape from day 0 of each year or account for leap years with unix timestamps

def get_annualized_submissions(subreddit, end_date, ini_date = None):

    unix_year = 31536000
    limit =  None
    
    if ini_date is None:
        ini_date_object = datetime.today()


    end_date_object = ciso8601.parse_datetime(end_date)
    first_day_e = datetime(end_date_object.year, 1,1)
    first_day_e_epoch = int(time.mktime(first_day_e.timetuple()))



    first_day_i = datetime(ini_date_object.year, 1,1)
    first_day_i_epoch = int(time.mktime(first_day_i.timetuple()))



    n_loops = (ini_date_object - end_date_object).days / 365

    if n_loops is not int:
        n_loops = int(n_loops) + 1


    end_date_epoch = int(time.mktime(end_date_object.timetuple()))
    ini_date_epoch = int(time.mktime(ini_date_object.timetuple()))

    add_remainder = ini_date_epoch - first_day_i_epoch

    unix_ts = np.flip(np.arange(0, n_loops) * unix_year)
    unix_ts = np.append(unix_ts, unix_ts[-1])
    unix_ts = ini_date_epoch - unix_ts
    unix_ts[:-1] = unix_ts[:-1] - add_remainder

    for i in unix_ts:
        i_date_str = datetime.fromtimestamp(i).strftime('%d-%m-%y')

    unix_ts = unix_ts.astype(int)

    if n_loops == 0:
        print("Range of dates provided is less than one year, only one dataset will be created")
        submissions = api.search_submissions(subreddit=subreddit, limit = limit, before = ini_date_epoch , after = end_date_epoch)
        sub_df = pd.DataFrame(submissions)

        i_date_str = datetime.fromtimestamp(ini_date_epoch).strftime('%d-%m-%y')
        e_date_str = datetime.fromtimestamp(end_date_epoch).strftime('%d-%m-%y') 


        print(f'{len(sub_df)} posts retrieved from Pushshift from {e_date_str} to {i_date_str}')
        
        sub_df.to_csv(f'{subreddit}_submissions_{e_date_str}_{i_date_str}.csv') 

        return sub_df


    for cnt, i  in enumerate(unix_ts[:-1]):
        dt_before = unix_ts[cnt + 1]
        dt_after = i
        
        i_date_str = datetime.fromtimestamp(dt_before).strftime('%d-%m-%y')
        print(i_date_str)
        e_date_str = datetime.fromtimestamp(dt_after).strftime('%d-%m-%y') 
        print(e_date_str)
        
        submissions = api.search_submissions(subreddit=subreddit, limit = limit, before = dt_before , after = dt_after)
        sub_df = pd.DataFrame(submissions)
        print(f'{len(sub_df)} posts retrieved from Pushshift from {e_date_str} to {i_date_str}')
        
        sub_df.to_csv(f'{subreddit}_submissions_{e_date_str}_{i_date_str}.csv') 
        
        if cnt == 0:
            sub_df_all = pd.DataFrame(submissions)
        else:
            sub_df_all = pd.concat([sub_df_all, sub_df])
        
   
        
    i_date_str = datetime.fromtimestamp(ini_date_epoch).strftime('%d-%m-%y')
    e_date_str = datetime.fromtimestamp(first_day_e_epoch).strftime('%d-%m-%y')    

    sub_df_all.to_csv(f'ALL_{subreddit}_submissions_{e_date_str}_{i_date_str}.csv') 

    
    return sub_df_all


def clean_data(dataframe):
    cols = dataframe.columns
    print(f"the dataset currently has {cols.shape} columns")
