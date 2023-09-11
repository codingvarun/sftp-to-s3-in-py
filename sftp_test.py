import pysftp
import boto3
import pandas as pd
import os,shutil
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

shutil.rmtree("sftp_downloads")
os.mkdir("sftp_downloads")
conn = pysftp.Connection(host="test.rebex.net",
                         username="demo",
                         password="password",
                         port=22)
long_listing = conn.listdir_attr(remotepath='.')
long_listing = pd.DataFrame([l.__dict__ for l in long_listing ])
long_listing.to_csv("sftp_downloads/file_metadata.csv",index=False)
for file in long_listing["filename"]:
    try:
        conn.get(f"./{file}",f"sftp_downloads/{file}",preserve_mtime=True)
    except IOError:
        print("IO Error has occured")
conn.close()    
s3 = boto3.client('s3')
bucketname='pysftp-bucket'
files = os.listdir("sftp_downloads")
print(files)
for file in files:
    file_key = f"sftp_downloads/{file}"
    response = s3.upload_file(Filename=file_key,
                            Bucket=bucketname,
                            Key=file_key)
    print(response)
s3.close()
