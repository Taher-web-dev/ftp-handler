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

    df = pd.DataFrame(data)
    f = io.StringIO()
    df.to_csv(f)
    bio = io.BytesIO(str.encode(f.getvalue()))

    ftp = FTP('ftp.lnpbermuda.org')
    print("log in to host")
    ftp.login("lnpber01", "LA04dpv1951")

    print("started file transfer...")
    ftp.storbinary(f"STOR {filename}", bio)

    ftp.close()

    return 'ftp done!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5001)
