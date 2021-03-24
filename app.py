import os
from threading import Thread
from datetime import datetime
import pysftp
from flask import Flask, request
import pandas as pd
import io
from ftplib import FTP

app = Flask(__name__)
ENV = os.environ.get('ENV')
if ENV == "PROD":
    lnp_single_record_directory = "/public_html/data/SingleRecord"
    lnp_last_update_dir = "/public_html/data/lastUpdate"
    lnp_history_dir = "/public_html/data/history"
    netnumber_directory = "/uploads"
    digicel_directory = "uploads"
else:
    lnp_single_record_directory = "/test_directory"
    lnp_last_update_dir = "/test_directory"
    lnp_history_dir = "/test_directory"
    netnumber_directory = "/recon"
    digicel_directory = "uploads_test"

DIRECTORIES = dict(lnp=dict(host="ftp.lnpbermuda.org", username="lnpber01", password="LA04dpv1951"),
                   netnumber=dict(),
                   digicel=dict())


@app.route('/')
def hello_world():
    print(ENV)
    print(lnp_single_record_directory)
    return 'Hello Worlds!'


@app.route('/', methods=['POST'])
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
    data = payload.get('data')
    target = payload.get('target')

    ftp_transfer_thread = Thread(target=all_ported_numbers_transfer_job, kwargs=dict(data=data, target=target))
    ftp_transfer_thread.start()

    return 'ftp done!'


def ftp_transfer_job(data, target, filename):
    print(f"FTP Job started for {target} - {filename}")
    res = ""
    for doc in data:
        for k, v in doc.items():
            res += f"{k} : {v} \n"
    if target == 'lnp':
        bio = io.BytesIO(str.encode(res))
        ftp = FTP('ftp.lnpbermuda.org')
        ftp.login("lnpber01", "LA04dpv1951")
        ftp.cwd(lnp_single_record_directory)
        ftp.storbinary(f"STOR {filename}", bio)
        print(f"finished file transfer for {filename}.")
        ftp.close()

    elif target == 'netnumber':
        with pysftp.Connection(host="ftp.netnumber.com", username="bmnp", private_key_pass="lnpbermuda",
                               private_key="/home/aziz/.ssh/id_rsa") as sftp:
            sftp.cwd(netnumber_directory)
            sftp.putfo(io.StringIO(res), filename)

    elif target == 'digicel':
        with pysftp.Connection(host="64.147.95.49", username="LNPBermuda", password="4mAuYfV8cstQezpw") as sftp:
            sftp.cwd(digicel_directory)
            sftp.putfo(io.StringIO(res), filename)

    print(f"{filename} Pushed to {target}")


def all_ported_numbers_transfer_job(data, target, ):
    print(f"All ported numbers Job started for {target} ")
    f = io.StringIO()
    df_cols = ['number', 'block_operator', 'block_operator_prefix', 'new_operator', 'new_operator_prefix',
               'number_porting', 'date_porting', 'date_porting_lbl', 'status']
    df = pd.DataFrame(data)
    df = df.loc[:, df_cols]
    df.to_csv(f)
    bio = io.BytesIO(str.encode(f.getvalue()))

    if target == 'lnp':
        ftp = FTP('ftp.lnpbermuda.org')
        ftp.login("lnpber01", "LA04dpv1951")
        # store latest file
        ftp.storbinary(f"STOR {lnp_last_update_dir}/ported_numbers.csv", bio)
        # store history
        ftp.storbinary(f'STOR {lnp_history_dir}/NPSported_numbers_{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}', bio)

        ftp.close()

        print(f"All ported numbers  Pushed to {target}")


# @app.route('/transactions', methods=['POST'])
# def save_all_transactions():
#     payload = request.get_json()
#     data = payload.get('data')
#     filename = payload.get('filename')
#     # df = pd.DataFrame(data)
#     # f = io.StringIO()
#     # df.to_csv(f)
#     res = ""
#     for doc in data:
#         for k, v in doc.items():
#             res += f"{k} : {v} \n"
#
#     bio = io.BytesIO(str.encode(res))
#
#     ftp = FTP('ftp.lnpbermuda.org')
#     print("log in to host")
#     ftp.login("lnpber01", "LA04dpv1951")
#
#     ftp.cwd(lnp_single_record_directory)
#     print("changed directory...")
#
#     print("started file transfer...")
#     ftp.storbinary(f"STOR {filename}", bio)
#     print(f"finished file transfer for {filename}.")
#
#     ftp.close()
#
#     return 'ftp done!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5001)
