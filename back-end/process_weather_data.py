import time
import pandas as pd
import psycopg2
import numpy as np
from psycopg2.extensions import AsIs
from db_utils import run_w3s_query
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import psycopg2.extras as extras
import itertools 
import os
import shutil
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from timeloop import Timeloop


tl = Timeloop()


@tl.job(interval=timedelta(seconds=11))
def intiate_jobs():
    """This is a scheduled scripts for sending emails
       for new jobs received since last checking time
    """
    # Step 1 ---- Find all new requests
    status = 'pending'
    params = {"status": status}
    query = ('SELECT job_id, start_date, end_date, email, ' 
             'data_type, data_format, var_flag '
             'FROM world_climate.requests AS requests ' 
             'WHERE requests.status = %(status)s')
    names, records = run_w3s_query(query, params=params)
    df_requests = pd.DataFrame(records, columns=names)
    
    # Step 2 ---- Send receipt email and update job status 
    for i in range(len(df_requests)):
        job_id = df_requests.loc[i, 'job_id']
        receiver_email = df_requests.loc[i, 'email']
        status = send_job_receipt_email(job_id, receiver_email) 
        params = {"status": status, "job_id": job_id}
        query = ('UPDATE world_climate.requests '
                 'SET status = %(status)s ' 
                 'WHERE job_id = %(job_id)s')
        _, _ = run_w3s_query(query, params=params)
    
    # Step 3 --- Update script logger
    print("{} new jobs queued into system at : {}".format(len(df_requests), time.ctime()))



@tl.job(interval=timedelta(minutes=1))    
def process_jobs():
    """Process any pending data jobs. 
    """

    # Step 1 ---- Process All Pending Requests
    status = 'queued'
    params = {"status": status}
    query = ('SELECT job_id, start_date, end_date, email, ' 
             'data_type, data_format, var_flag '
             'FROM world_climate.requests AS requests ' 
             'WHERE requests.status = %(status)s')
    names, records = run_w3s_query(query, params=params)
    df_requests = pd.DataFrame(records, columns=names)
    
    for i in range(len(df_requests)):
        job_id = df_requests.loc[i, 'job_id']
        process_request(job_id)

    # Step 2 --- Update script logger
    print("{} data jobs processed at : {}".format(len(df_requests), time.ctime()))    


@tl.job(interval=timedelta(minutes=120))
def clean_jobs():
    """Close all finished jobs that have been in system for 
       more than a week, and delete associated files
    """

    # Step 1 ---- Clean All Finished Requests (change status to closed)
    status = 'finished'
    params = {"status": status}
    query = ('SELECT job_id, fin_time '
             'FROM world_climate.requests AS requests ' 
             'WHERE requests.status = %(status)s')
    names, records = run_w3s_query(query, params=params)
    df_requests = pd.DataFrame(records, columns=names)
    for i in range(len(df_requests)):
        job_id = df_requests.loc[i, 'job_id']
        fin_time = df_requests.loc[i, 'fin_time']
        current_time = datetime.now()
        diff = (current_time - fin_time).days
        if diff >= 7:
            status = 'closed'
            params = {"status": status, "job_id": job_id}
            query = ('UPDATE world_climate.requests '
                     'SET status = %(status)s ' 
                     'WHERE job_id = %(job_id)s')
            _, _ = run_w3s_query(query, params=params)

    # Step 2 --- Update script logger
    print("{} finished data jobs closed at : {}".format(len(df_requests), time.ctime()))    


def send_job_receipt_email(job_id, receiver_email):

    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "uog.water@gmail.com"  # Enter your address
    password = input("Type your password and press enter: ")
    subject = "Processing W3S Data Request Number: " + job_id
    body = """\
Dear Sir/Madam,

Your request for climate data from the W3S platform has been successfully submitted. \
You can check and monitor the status of the submitted request on the \
Track Requests tab on the W3S platform (https://www.uoguelph.ca/watershed/w3s/).
  
Please use your email to check the status of all pending requests you submitted. \
Once your request is processed, we will send you an email confirming its completion. \
You may download the requested data from the above link then. Kindly use your email \
and jobId for retrieving the requested data. Your jobId is {}. 
    
Best Regards,
W3S Team

    """.format(job_id)   
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))
    text = message.as_string()

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, text)
        status = "queued"
    except:
        status = "error-email"

    return status


def send_job_completion_email(job_id, receiver_email):

    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "uog.water@gmail.com"  # Enter your address
    password = input("Type your password and press enter: ")
    subject = "W3S Request Complete - Req. Number: " + job_id
    body = """\
Dear Sir/Madam,

Your request (JobId {}) for climate data from the W3S platform has been successfully processed. \
You can download the requisite data from the \
Track Requests tab on the W3S platform (https://www.uoguelph.ca/watershed/w3s/).
  
Kindly use your email and jobId for retrieving the requested data. Your jobId is {}. 

Thank you for using the W3S platform for your hydro-climate data requests. Please write to us \
at uog.water@gmail.com for feedback / queries. 
    
Best Regards,
W3S Team

    """.format(job_id, job_id)   
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))
    text = message.as_string()

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, text)
        status = "finished"
    except:
        status = "error-email"

    return status


def process_request(job_id):

    # Step 0 ---- Get details of submitted job
    params = {"job_id": job_id}
    query = ('SELECT job_id, start_date, end_date, email, ' 
             'data_type, data_format, var_flag '
             'FROM world_climate.requests AS requests ' 
             'WHERE requests.job_id = %(job_id)s')
    names, records = run_w3s_query(query, params=params)
    df_request = pd.DataFrame(records, columns=names)
    dates = []
    dates.append(datetime.strptime(df_request.loc[0, "start_date"], '%Y-%m-%d').date())
    dates.append(datetime.strptime(df_request.loc[0, "end_date"], '%Y-%m-%d').date())
    job_id = df_request.loc[0, 'job_id']
    job_email = df_request.loc[0, 'email']
    path = '/Users/taimoorakhtar/Documents/Datasets/w3s_data/'
    folder_path = path + job_id
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

    # Step 1 ---- Process job to create data files
    if df_request.loc[0, "data_type"] == "Historical":
        if df_request.loc[0, "var_flag"] == 1:
            df_prec_pts, prec_data = get_prec_data(job_id, dates)
            if df_request.loc[0, "data_format"] == "SWAT":
                prepare_prec_swat(df_request, df_prec_pts, prec_data)
            else:
                prepare_prec_csv(df_request, df_prec_pts, prec_data)
        
        elif df_request.loc[0, "var_flag"] == 2:
            df_prec_pts, prec_data = get_prec_data(job_id, dates)
            df_temp_pts, df_temp_data = get_cpc_data(job_id, dates)
            if df_request.loc[0, "data_format"] == "SWAT":
                prepare_prec_swat(df_request, df_prec_pts, prec_data)
                prepare_temp_swat(df_request, df_temp_pts, df_temp_data)
            else:
                prepare_prec_csv(df_request, df_prec_pts, prec_data)
                prepare_temp_csv(df_request, df_temp_pts, df_temp_data)
        
        elif df_request.loc[0, "var_flag"] == 3:
            df_temp_pts, df_temp_data = get_cpc_data(job_id, dates)
            if df_request.loc[0, "data_format"] == "SWAT":
                prepare_temp_swat(df_request, df_temp_pts, df_temp_data)
            else:
                prepare_temp_csv(df_request, df_temp_pts, df_temp_data)

        # Write created files to zip
        file_path = folder_path + '/files/'
        output_file = folder_path + '/' + 'historicalData'
        shutil.make_archive(output_file, 'zip', file_path)

        # Send email and update archive
        output_file = output_file + '.zip'
        current_time = datetime.now()
        if os.path.exists(output_file):
            status = send_job_receipt_email(job_id, job_email)
        else:
            status = 'error-data'
        params = {"status": status, "cur_time": current_time, "job_id": job_id}
        query = ('UPDATE world_climate.requests '
                 'SET status = %(status)s, fin_time = %(cur_time)s ' 
                 'WHERE job_id = %(job_id)s')
        _, _ = run_w3s_query(query, params=params)


def prepare_prec_swat(df_request, df_pts, prec_data):
    
    # Step 0 ---- Create folder to save data in
    job_id = df_request.loc[0, 'job_id']
    start_date = df_request.loc[0, 'start_date'].replace('-', '')
    path = '/Users/taimoorakhtar/Documents/Datasets/w3s_data/'
    folder_path = path + job_id + '/files'
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    
    # Step 1 ---- Prepare input files
    df_pts_final = df_pts[['lat', 'long', 'elevation']]
    df_pts_final.columns = ['LAT', 'LONG', 'ELEVATION']
    df_pts_final.insert(0, "NAME", '')
    for i in range(len(df_pts_final)):
        df_pts_final.loc[i, 'NAME'] = ('pcp' + str(df_pts_final.loc[i, 'LONG']) + 
                                      '_' + str(df_pts_final.loc[i, 'LAT']))
        file_name = folder_path + '/' + df_pts_final.loc[i, 'NAME'] + '.txt'
        my_data = np.asarray(prec_data[i])
        with open(file_name,'a') as my_file:
            my_file.write(start_date)
            my_file.write("\n")
            np.savetxt(my_file, my_data, fmt='%.1f')

    df_pts_final.to_csv(folder_path + '/pcp.txt', header=True, index=False, sep=',')

    

def prepare_prec_csv(df_request, df_pts, prec_data):
    
    # Step 0 ---- Create folder to save data in
    job_id = df_request.loc[0, 'job_id']
    start_date = datetime.strptime(df_request.loc[0, "start_date"], '%Y-%m-%d').date()
    end_date = datetime.strptime(df_request.loc[0, "end_date"], '%Y-%m-%d').date()
    date_array = list(start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1))
    path = '/Users/taimoorakhtar/Documents/Datasets/w3s_data/'
    folder_path = path + job_id + '/files'
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    
    # Step 1 ---- Prepare input files
    file_name = folder_path + '/precipitation.csv'
    print(len(df_pts))
    for i in range(len(df_pts)):
        df = pd.DataFrame(columns=['date', 'lat', 'long', 'elev', 'precip'])
        df['date'] = date_array
        df['lat'] = df_pts.loc[i, 'lat']
        df['long'] = df_pts.loc[i, 'long']
        df['elev'] = df_pts.loc[i, 'elevation']
        df['precip'] = prec_data[i]
        if i == 0:
            df.to_csv(file_name, index=False)
        else:
            df.to_csv(file_name, mode='a', header=False, index=False)


def prepare_temp_csv(df_request, df_pts, df_data):

    # Step 0 ---- Create folder to save data in
    job_id = df_request.loc[0, 'job_id']
    start_date = datetime.strptime(df_request.loc[0, "start_date"], '%Y-%m-%d').date()
    end_date = datetime.strptime(df_request.loc[0, "end_date"], '%Y-%m-%d').date()
    date_array = list(start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1))
    path = '/Users/taimoorakhtar/Documents/Datasets/w3s_data/'
    folder_path = path + job_id + '/files'
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    
    # Step 1 ---- Prepare input files
    file_name = folder_path + '/temperature.csv'
    for i in range(len(df_pts)):
        df = pd.DataFrame(columns=['date', 'lat', 'long', 'elev', 'tmin', 'tmax'])
        df['date'] = date_array
        df['lat'] = df_pts.loc[i, 'lat']
        df['long'] = df_pts.loc[i, 'long']
        df['elev'] = df_pts.loc[i, 'elevation']
        gid = df_pts.loc[i, 'gid']
        my_data = df_data.loc[df_data["gid"] == str(gid), ["tmax", "tmin"]].to_numpy()
        df['tmax'] = my_data[:,0]
        df['tmin'] = my_data[:,1]
        if i == 0:
            df.to_csv(file_name, index=False)
        else:
            df.to_csv(file_name, mode='a', header=False, index=False)



def prepare_temp_swat(df_request, df_pts, df_data):
    
    # Step 0 ---- Create folder to save data in
    job_id = df_request.loc[0, 'job_id']
    start_date = df_request.loc[0, 'start_date'].replace('-', '')
    print(start_date)
    path = '/Users/taimoorakhtar/Documents/Datasets/w3s_data/'
    folder_path = path + job_id + '/files'
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    
    # Step 1 ---- Prepare input files
    df_pts_final = df_pts[['lat', 'long', 'elevation']]
    df_pts_final.columns = ['LAT', 'LONG', 'ELEVATION']
    df_pts_final.insert(0, "NAME", '')
    for i in range(len(df_pts_final)):
        df_pts_final.loc[i, 'NAME'] = ('tmp' + str(df_pts_final.loc[i, 'LONG']) + 
                                      '_' + str(df_pts_final.loc[i, 'LAT']))
        file_name = folder_path + '/' + df_pts_final.loc[i, 'NAME'] + '.txt'
        gid = df_pts.loc[i, 'gid']
        my_data = df_data.loc[df_data["gid"] == str(gid), ["tmax", "tmin"]].to_numpy()
        with open(file_name,'a') as my_file:
           my_file.write(start_date)
           my_file.write("\n")
           np.savetxt(my_file, my_data, fmt='%.1f', delimiter=',')

    df_pts_final.to_csv(folder_path + '/tmp.txt', header=True, index=False, sep=',')


def get_prec_data(job_id, dates):

    params = {"job_id": job_id}
    # Step 1 ---- Get list of points in selected shape (W3S)
    query = ('SELECT prec_coords.lat AS lat, prec_coords.long AS long, ' 
                    'prec_coords.elevation AS elevation, prec_coords.gid AS gid ' 
             'FROM world_climate.prec_coords AS prec_coords ' 
             'JOIN world_climate.requests AS requests ' 
             'ON ST_Contains(requests.geom, prec_coords.geom) ' 
             'WHERE requests.job_id = %(job_id)s')
    tic = time.perf_counter()
    names, records = run_w3s_query(query, params=params)
    if len(records) == 0:
        query = ('SELECT prec_coords.lat AS lat, prec_coords.long AS long, ' 
                        'prec_coords.elevation AS elevation, prec_coords.gid AS gid ' 
                 'FROM world_climate.prec_coords AS prec_coords ' 
                 'ORDER BY prec_coords.geom <-> ' 
                 '(SELECT geom FROM world_climate.requests AS requests ' 
                 'WHERE requests.job_id = %(job_id)s) LIMIT 1')
        names, records = run_w3s_query(query, params=params)
    df = pd.DataFrame(records, columns=names)
    toc = time.perf_counter()
    print(f"Finished reading points in {toc - tic:0.4f} seconds")

    # Step 2 ---- Get GPM precipitation data records for specified dates
    start_date = dates[0].replace(day=1)
    end_date = dates[1].replace(day=1)
    difference = relativedelta(end_date, start_date)
    date_array = (start_date + relativedelta(months=x) for x in range(0, difference.years*12 + difference.months + 1))
    date_array = tuple(date_array)
    tic = time.perf_counter()
    query = ('SELECT date, gid, prec FROM world_climate.prec_data '
             'WHERE gid IN %(gid)s and date IN %(dates)s ORDER BY date')
    params = params = {"gid": tuple(df["gid"].to_list()), "dates": date_array}
    names, records = run_w3s_query(query, params)
    toc = time.perf_counter()
    print(f"Finished reading data in {toc - tic:0.4f} seconds")

    # Step 3 ---- Reformat precipitation data into a list of lists
    tic = time.perf_counter()
    start_shift = (dates[0] - start_date).days
    end_shift = ((end_date + relativedelta(months=1)) - dates[1]).days - 1
    prec_data = []
    for i in range(len(df)):
        gid = df.loc[i, "gid"]
        my_data = [record[2] for record in records if record[1] == gid]
        my_data = list(itertools.chain.from_iterable(my_data))
        if end_shift == 0:
            my_data = my_data[start_shift:]
        else:
            my_data = my_data[start_shift:-end_shift]
        prec_data.append(my_data)
    toc = time.perf_counter()
    print(f"Finished converting data in {toc - tic:0.4f} seconds")

    
    return df, prec_data


def get_cpc_data(job_id, dates):

    params = {"job_id": job_id}
    # Step 1 ---- Get list of points in selected shape (W3S)
    query = ('SELECT temp_coords.lat AS lat, temp_coords.long AS long, ' 
                    'temp_coords.elevation AS elevation, temp_coords.gid AS gid ' 
             'FROM world_climate.temp_coords AS temp_coords ' 
             'JOIN world_climate.requests AS requests ' 
             'ON ST_Contains(requests.geom, temp_coords.geom) ' 
             'WHERE requests.job_id = %(job_id)s')
    tic = time.perf_counter()
    names, records = run_w3s_query(query, params=params)
    if len(records) == 0:
        query = ('SELECT temp_coords.lat AS lat, temp_coords.long AS long, ' 
                        'temp_coords.elevation AS elevation, temp_coords.gid AS gid ' 
                 'FROM world_climate.temp_coords AS temp_coords ' 
                 'ORDER BY temp_coords.geom <-> ' 
                 '(SELECT geom FROM world_climate.requests AS requests ' 
                 'WHERE requests.job_id = %(job_id)s) LIMIT 1')
        names, records = run_w3s_query(query, params=params)
    df = pd.DataFrame(records, columns=names)
    toc = time.perf_counter()
    print(f"Finished reading points in {toc - tic:0.4f} seconds")

    # Step 2 ---- Get CPC temperature data records for specified dates and average over cells
    start_date = dates[0]
    end_date = dates[1]
    date_array = (start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1))
    date_array = list(date_array)
    date_strings = [date_obj.strftime('%Y-%m-%d') for date_obj in date_array]
    tic = time.perf_counter()
    query = 'SELECT date, gid, tmin, tmax FROM world_climate.temp_data WHERE gid IN %(gid)s and date IN %(dates)s ORDER BY date'
    gid_list = [str(gid) for gid in df["gid"].to_list()]
    params = params = {"gid": tuple(gid_list), "dates": tuple(date_strings)}
    names, records = run_w3s_query(query, params)
    df_data = pd.DataFrame(records, columns=names)
    toc = time.perf_counter()
    print(f"Finished reading data in {toc - tic:0.4f} seconds")

    return df, df_data


    
if __name__ == "__main__":
   tl.start(block=True)