from flask import Flask, request
import pandas as pd
import io
from ftplib import FTP

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello Worlds!'


@app.route('/', methods=['POST'])
def push_file():
    payload = request.get_json()
    data = payload.get('data')
    filename = payload.get('filename')

    # df = pd.DataFrame(data)
    # f = io.StringIO()
    # df.to_csv(f)
    res = ""
    for doc in data:
        for k, v in doc.items():
            res += f"{k} : {v} \n"

    bio = io.BytesIO(str.encode(res))

    ftp = FTP('ftp.lnpbermuda.org')
    print("log in to host")
    ftp.login("lnpber01", "LA04dpv1951")

    ftp.cwd("/test_directory")
    print("changed directory...")

    print("started file transfer...")
    ftp.storbinary(f"STOR {filename}", bio)
    print(f"finished file transfer for {filename}.")

    ftp.close()

    return 'ftp done!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5001)
