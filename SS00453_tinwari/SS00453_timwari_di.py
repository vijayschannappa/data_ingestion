from pathlib import Path
import os
import sys
from ftplib import FTP
from datetime import datetime, timedelta
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

dt = datetime.now().date()

delta = dt - timedelta(days=1)

dt_str = delta.strftime(format="%d-%m-%Y")


query = f"modifiedDate>'{delta}'"

param = {}

param['q'] = query

parent_dir = Path(__file__).resolve().parent

os.chdir(parent_dir)

gauth = GoogleAuth()
gauth.CommandLineAuth()
drive = GoogleDrive(gauth)

ftp_creds = dict(host='52.41.144.51',
                 passwd='agoml1412', user='moserbb')


def fetch_ss_file():
    change_working_directory('data_store')
    file_name = file_download()
    os.chdir(parent_dir)


def file_download():
    file_list = drive.ListFile({**param}).GetList()
    for file in file_list:
        try:
            file.GetContentFile(file['title'])
            # print(f'file downloaded: {file}')
            file_name = file['title']
            push_to_ftp(file_name)
        except Exception as e:
            continue
    return file_name


def push_to_ftp(file_name):
    ftp = FTP(**ftp_creds)
    root_dir = "/moser"
    ftp.cwd(root_dir)
    # if check_for_dir(ftp) is True:
    # ftp.mkd(dt_str)
    file = open(file_name, 'rb')
    ftp.storbinary(f'STOR {file_name}', file)
    print(f'file_upload:{file}')
    file.close()
    ftp.cwd('/')
    ftp.quit()


def check_for_dir(ftp):
    dir_list = []
    ftp.retrlines('LIST', dir_list.append)
    for fold in dir_list:
        if fold.split()[-1] == dt_str:
            return False
    return True


def change_working_directory(data_store):
    ss_dir = f'{parent_dir}/data_store/'
    if not os.path.exists(ss_dir):
        os.makedirs(ss_dir)
    os.chdir(ss_dir)


if __name__ == '__main__':
    fetch_ss_file()


#*/10 * * * * /home/ubuntu/.pyenv/versions/pipelines/bin/python /home/ubuntu/external_pipelines/scada_data_push_ftp/gdrive_di/SS00390/gdrive_ss00390.py
