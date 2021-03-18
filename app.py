from flask import Flask, request
import pandas as pd
import io
from ftplib import FTP

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/', methods=['POST'])
def push_file():
    # payload = request.get_json()
    # data = payload.get('data')
    data = dict(
        a=1,
        b=2,
        c=3
    )
    df = pd.DataFrame(data)
    f = io.StringIO()
    df.to_csv(f)
    bio = io.BytesIO(str.encode(f.getvalue()))

    ftp = FTP('ftp.lnpbermuda.org')
    ftp.login("lnpber01", "LA04dpv1951")
    ftp.storbinary('test_100_portings.csv', bio)

    ftp.close()

    return 'ftp done!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5001)
