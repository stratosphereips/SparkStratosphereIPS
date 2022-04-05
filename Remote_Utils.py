import base64
import os
import subprocess
import sys
from datetime import date
import pandas as pd
from Crypto.Cipher import Blowfish
import Constants


def copy_files():
    proc = subprocess.Popen(['hdfs', 'dfs', '-ls', 'flows_from_list'], stdout=subprocess.PIPE)

    # get files with non-zero size
    stdout = proc.stdout.readlines()
    splitted = list(map(lambda x: x.split(), stdout))
    filtered = list(filter(lambda x: (len(x) < 9) and (len(x) > 4), splitted))
    not_empty = list(filter(lambda x: str(x[4], 'utf-8') != '0', filtered))
    filenames = list(map(lambda x: str(x[7], 'utf-8'), not_empty))

    # command to execute: 'hdfs dfs -get files_to_get target_dir'
    command = ['hdfs', 'dfs', '-get']
    command.extend(filenames)
    command.append(Constants.HOMEDIR + Constants.RESULT_DIR)

    proc = subprocess.Popen(command,
                            stdout=subprocess.DEVNULL)
    # end process
    proc.communicate('S\n')


def clean_results_before():
    result_folder = Constants.HOMEDIR + Constants.RESULT_DIR
    if os.path.isdir(result_folder):
        for file_name in os.listdir(result_folder):
            file = os.path.join(result_folder, file_name)
            try:
                os.unlink(file)
            except Exception as e:
                print(e)


def generate_report():
    pass


def clean_IoC_file(dataframe):
    current_date = date.today()
    month = current_date.month
    lower_bound = current_date.replace(month=(month - 2))
    current_date = current_date.strftime("%m/%d/%Y")
    lower_bound = lower_bound.strftime("%m/%d/%Y")
    return dataframe.loc[lower_bound: current_date]


def add_from_temp_to_IoC():
    ioc_path = Constants.HOMEDIR + Constants.RESOURCE_DIR + Constants.IOC_FILE
    tmp_file_path = Constants.HOMEDIR + Constants.RESOURCE_DIR + sys.argv[1]
    tmp_df = pd.read_csv(tmp_file_path, header=None, names=['IP', 'Date'])
    tmp_df = tmp_df.fillna(pd.to_datetime('today'))
    with open(ioc_path, 'a') as f:
        tmp_df.to_csv(f, header=None, index=False)


def clean_up_after():
    proc = subprocess.Popen(['hdfs', 'dfs', '-rm', '-r', Constants.HDFS_RESULT_DIR])
    proc.communicate('S\n')
    temp_file = Constants.HOMEDIR + Constants.RESOURCE_DIR + sys.argv[1]
    if os.path.isfile(temp_file):
        os.remove(temp_file)


def decrypt_results():
    files = os.listdir(Constants.HOMEDIR + Constants.RESULT_DIR)
    key = b'(,}65dFep_hZJ7)v-^EYhG9a(5DEp<W-!^99?7_Y%U8kp(sZrF&:#L'


    def decrypt_data(encrypted_data):
        encrypted_data = base64.b64decode(encrypted_data)
        bs = Blowfish.block_size
        iv = encrypted_data[:bs]
        encrypted_data = encrypted_data[bs:]
        cipher = Blowfish.new(key, Blowfish.MODE_CBC, iv)
        decrypted = cipher.decrypt(encrypted_data)
        last_byte = decrypted[-1]
        decrypted = decrypted[:- (last_byte if type(last_byte) is int else ord(last_byte))]
        return decrypted


    header = 'StartTime,Dur,Proto,SrcAddr,Sport,Dir,DstAddr,Dport,State,sTos,dTos,TotPkts,TotBytes,SrcBytes' \
             ',SrcPkts,Label,srcUdata,dstUdata,_StartTime,_year,_month,_day'
    columns = header.split(',')
    for file in files:
        # print(file)
        full_path = Constants.HOMEDIR + Constants.RESULT_DIR + file
        df = pd.read_csv(full_path, header=None, names=columns)
        df['srcUdata'] = df['srcUdata'].apply(lambda x: decrypt_data(x))
        df['dstUdata'] = df['dstUdata'].apply(lambda x: decrypt_data(x))
        df.to_csv(full_path, index=False)
