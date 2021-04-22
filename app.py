import csv
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from threading import Thread
from datetime import datetime

import firebase_admin
import pysftp
from firebase_admin import credentials, firestore
from flask import Flask, request
import pandas as pd
import io
from ftplib import FTP

from google.cloud import bigquery, storage
from google.cloud.firestore import Query
from google.cloud.firestore_v1 import Client

app = Flask(__name__)
ENV = os.environ.get('ENV')
LNP_HOST = 'ftp.lnpbermuda.org'
LNP_USER = "lnpber01"
LNP_PASSWORD = "LA04dpv1951"
gmail_access_email: str = "rmp@horrockslnp.net"
gmail_access_password: str = 'p8P.~b6j!)<m2"8%'
bq = bigquery.Client()
gcs = storage.Client()

if ENV == "PROD":
    lnp_single_record_directory = "/public_html/data/SingleRecord"
    lnp_last_update_dir = "/public_html/data/lastUpdate"
    lnp_history_dir = "/public_html/data/history"
    netnumber_directory = "/uploads"
    digicel_directory = "updates"
    fb_cred_path = "./secrets/lnpbermuda-prod-firebase-adminsdk-ovf8g-a4685962de.json"
    cell_filename = "/public_ftp/ported_numbers.csv"
    project_name = "lnpbermuda-prod"

else:
    lnp_single_record_directory = "/test_directory"
    lnp_last_update_dir = "/test_directory"
    lnp_history_dir = "/test_directory"
    netnumber_directory = "/recon"
    digicel_directory = "uploads_test"
    fb_cred_path = "./secrets/lnpbermuda-dev-firebase-adminsdk-125rr-bad1f123e9.json"
    cell_filename = "/test_directory/ported_numbers_TEST.csv"
    project_name = "lnpbermuda-dev"

cred = credentials.Certificate(fb_cred_path)
firebase_admin.initialize_app(cred)
dbf: Client = firestore.client()

DIRECTORIES = dict(lnp=dict(host="ftp.lnpbermuda.org", username="lnpber01", password="LA04dpv1951"),
                   netnumber=dict(),
                   digicel=dict())


@app.route('/')
def hello_world():
    print(ENV)
    print(lnp_single_record_directory)
    return 'Hello Worlds!'


@app.route('/single_doc', methods=['POST'])
def push_file():
    payload = request.get_json()
    data = payload.get('data')
    filename = payload.get('filename')
    target = payload.get('target')

    ftp_transfer_thread = Thread(target=ftp_transfer_job, kwargs=dict(data=data, filename=filename, target=target))
    ftp_transfer_thread.start()

    return 'ftp done!'


@app.route('/all_portings', methods=['POST'])
def push_all_portings_file():
    payload = request.get_json()
    target = payload.get('target')
    print(f"STARTING ALL PORTINGS TRANSFER THREAD!")
    ftp_transfer_thread = Thread(target=all_ported_numbers_transfer_job, kwargs=dict(target=target))
    ftp_transfer_thread.start()

    return 'ftp done!'


def ftp_transfer_job(data, target, filename):
    print(f"FTP Job started for {target} - {filename}")
    res = ""
    status = True
    error = ""

    if target != 'netnumber':
        for doc in data:
            for k, v in doc.items():
                res += f"{k} : {v} \n"
    elif target == 'netnumber':
        for doc in data:
            for k, v in doc.items():
                res += f"{v},"
        res = res[:-1]

    if target == 'lnp':
        bio = io.BytesIO(str.encode(res))
        try:
            ftp = FTP(LNP_HOST)
            ftp.login(LNP_USER, LNP_PASSWORD)
            ftp.cwd(lnp_single_record_directory)
            ftp.storbinary(f"STOR {filename}", bio)
            print(f"finished file transfer for {filename}.")
            ftp.close()
        except Exception as e:
            status = False
            error = str(e)
            print(f"EXCEPTION at SINGLE RECORD TRANSFER for {target} => {str(e)}")

    elif target == 'netnumber':
        try:
            with pysftp.Connection(host="ftp.netnumber.com", username="bmnp", private_key_pass="lnpbermuda",
                                   private_key="/home/aziz/.ssh/id_rsa") as sftp:
                sftp.cwd(netnumber_directory)
                sftp.putfo(io.StringIO(res), filename)
        except Exception as e:
            status = False
            error = str(e)
            print(f"EXCEPTION at SINGLE RECORD TRANSFER for {target} => {str(e)}")

    elif target == 'digicel':
        try:
            with pysftp.Connection(host="64.147.95.49", username="LNPBermuda", password="4mAuYfV8cstQezpw") as sftp:
                sftp.cwd(digicel_directory)
                sftp.putfo(io.StringIO(res), filename)
        except Exception as e:
            status = False
            error = str(e)
            print(f"EXCEPTION at SINGLE RECORD TRANSFER for {target} => {str(e)}")

    if status is False:
        send_email(subject="FTP transfer Failed!",
                   body=f"ftp transfer failed for file {filename} check logs for more information.",
                   addresses=["aziz@tanitlab.com"],
                   filename=filename)

    log_ftp(filename=filename, target=target, type="SINGLE_DOC", datetime=str(datetime.now()),
            status=status, error=error)
    print(f"{filename} Pushed to {target}")


def all_ported_numbers_transfer_job(target):
    print(f"All ported numbers Job started for {target} ")
    data = dbf.collection('portings').order_by("date_porting", direction=Query.ASCENDING).stream()
    data2 = dbf.collection('portings').order_by("date_porting", direction=Query.DESCENDING).stream()
    data = [d.to_dict() for d in data]
    data2 = [d2.to_dict() for d2 in data2]
    status = True
    error = None
    f = io.StringIO()
    f2 = io.StringIO()
    df_cols = ['number', 'block_operator', 'block_operator_prefix', 'new_operator', 'new_operator_prefix',
               'number_porting', 'date_porting', 'date_porting_lbl', 'status']

    df = pd.DataFrame(data)
    df2 = pd.DataFrame(data2)

    df = df.loc[:, df_cols]
    df2 = df2.loc[:, df_cols]

    print("DF1")
    print("---------------------------------------------")
    print(df)
    print("DF2")
    print("---------------------------------------------")
    print(df2)

    df.to_csv(f, index=False)
    df2.to_csv(f2, index=False, quoting=csv.QUOTE_ALL)

    gcs.get_bucket("lnp-files").blob("/ported_numbers/ported_numbers.csv") \
        .upload_from_file(f, content_type='text/csv')
    gcs.get_bucket("lnp-files").blob("/ported_numbers/ported_numbers2.csv") \
        .upload_from_file(f2, content_type='text/csv')
    # bio_latest = io.BytesIO(str.encode(f.getvalue()))
    # bio_history = io.BytesIO(str.encode(f.getvalue()))
    # bio_cell = io.BytesIO(str.encode(f2.getvalue()))
    # date = datetime.now()
    # if target == 'lnp':
    #     try:
    #         ftp = FTP(LNP_HOST)
    #         ftp.login(LNP_USER, LNP_PASSWORD)
    #         # store latest file
    #         ftp.storbinary(f"STOR {lnp_last_update_dir}/ported_numbers.csv", bio_latest)
    #         # store latest file CELL
    #         ftp.storbinary(f"STOR {cell_filename}", bio_cell)
    #         # store history
    #         filename = f'NPSported_numbers_{date.strftime("%Y-%m-%d_%H:%M:%S")}.csv'
    #         ftp.storbinary(f'STOR {lnp_history_dir}/{filename}', bio_history)
    #
    #         ftp.close()
    #
    #         print(f"All ported numbers  Pushed to {target}")
    #     except Exception as e:
    #         status = False
    #         error = str(e)
    #         print(f"EXCEPTION at PORTING LIST TRANSFER for {target} => {str(e)}")
    #
    # log_ftp(filename="ported_numbers", target=target, type="ALL_PORTINGS", datetime=str(date), status=status,
    #         error=error)


def log_ftp(**data):
    errors = bq.insert_rows_json(f"{project_name}.logs.ftp_ce", [data])
    if len(errors) > 0:
        print("LOGS_ERROR: failed to log FTP.")
        print(f"LOGS_ERROR: {errors}")


def send_email(subject: str, body: str, addresses: [str], filename=None):
    print(f"EMAIL: sending {subject} to {addresses}  || filename: {filename}")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = gmail_access_email
    msg["To"] = ", ".join(addresses)
    msg.attach(MIMEText(body, "html"))
    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.ehlo()
    server.login(gmail_access_email, gmail_access_password)
    print(addresses)

    try:
        server.sendmail(from_addr=gmail_access_email, to_addrs=addresses, msg=msg.as_string())
    except Exception as e:

        print(f"EMAIL SENDING FAILED:  subject: {subject} | addresses: {addresses}")
        print(f"EMAIL SENDING FAILED EXCEPTION:  {str(e)}")


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5001)
