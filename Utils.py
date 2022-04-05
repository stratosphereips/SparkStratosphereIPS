__author__ = 'username'
import paramiko
import os
import shutil
import Constants
from datetime import datetime
import tempfile
from os.path import basename
import getpass
from collections import defaultdict


def get_username():
    username_file = os.path.expanduser(Constants.USERNAME_FILE)
    if os.path.exists(username_file):
        with open(username_file, 'r') as f:
            username = f.readline()
        print('Using username: \'{}\'. If you want to run with a different username remove file {}'.format(username,
                                                                                                           username_file))
    else:
        username = input("Username: ")
        with open(username_file, 'w') as f:
            f.write(username)
    return username


def get_ssh_connection(username):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect('147.32.80.69', port=8000, username=username, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
    except paramiko.ssh_exception.SSHException:
        password = getpass.getpass('Your password: ')
        ssh.connect('147.32.80.69', port=8000, username=username, password=password, look_for_keys=False,
                    allow_agent=False)
    return ssh


def get_files_from_remote(ssh_connection, username):
    result_folder = Constants.RESULT_DIR + username + '/'
    if os.path.isdir(result_folder):
        shutil.rmtree(result_folder)
    os.makedirs(result_folder)
    sftp = ssh_connection.open_sftp()
    remote_dir = Constants.HOMEDIR + Constants.RESULT_DIR
    dir_items = sftp.listdir_attr(remote_dir)
    no_files = True
    for item in dir_items:
        remote_path = remote_dir + item.filename
        local_path = result_folder + item.filename
        sftp.get(remote_path, local_path)
        no_files = False
    if no_files:
        print('There are no records for this search')
    else:
        print('Found something. Check {} folder for results.'.format(result_folder))
    sftp.close()


def upload_files_to_remote(ssh_connection):
    sftp = ssh_connection.open_sftp()
    destination = Constants.PYTHON_CODE_DIR
    for file in Constants.FILES_TO_UPLOAD:
        sftp.put(file, destination + file)
    sftp.close()


def upload_tmp_file_to_remote(ssh_connection, file_path):
    sftp = ssh_connection.open_sftp()
    destination = Constants.HOMEDIR + Constants.RESOURCE_DIR
    sftp.put(file_path, destination + basename(file_path))
    sftp.close()


def create_temp_file(ip_list):
    if not os.path.isdir(Constants.RESOURCE_DIR):
        os.makedirs(Constants.RESOURCE_DIR)
    if (len(ip_list) == 1) & (os.path.isfile(ip_list[0])):
        return False, ip_list[0]

    fd, path = tempfile.mkstemp(dir=Constants.RESOURCE_DIR)
    today_date = datetime.today().strftime("%m/%d/%Y")
    if (len(ip_list) == 1) & (os.path.isfile(ip_list[0])):
        return ip_list[0]
    with open(fd, 'w') as f:
        for ip_with_comma in ip_list:
            for ip in ip_with_comma.split(','):
                if ip:
                    f.write(ip + '\n')

    return True, path


def is_authorised(ssh_connection, debug=False):
    _, ssh_stdout, std_err = ssh_connection.exec_command('klist')
    exit_status = ssh_stdout.channel.recv_exit_status()
    if exit_status != 0:
        return False
    output_lines = ssh_stdout.readlines()
    if debug:
        print('DEBUG:\n==================\nis_authorized')
        print(''.join(output_lines))
        print('===================')
        print(''.join(std_err.readlines()))
        print('===================')
    output_lines = [line for line in output_lines if line != '\n']
    ticket_end_date_str = output_lines[3].split('  ')[1]
    return datetime.today().strftime('%m/%d/%Y %H:%M:%S') < ticket_end_date_str


def create_krb_ticket(ssh_connection, password):
    _, ssh_stdout, std_err = ssh_connection.exec_command('echo \'{}\' | kinit stratosphere'.format(password))
    exit_status = ssh_stdout.channel.recv_exit_status()
    return exit_status


def is_cluster_busy(ssh_connection):
    _, stdout, std_err = ssh_connection.exec_command('yarn application -list')
    _ = stdout.channel.recv_exit_status()
    number_of_tasks = stdout.readlines()[0].strip()[-1]
    return number_of_tasks != '0'


def create_command_for_ssh(python_command, first_arg=''):
    return '{cd_to_dir} {python_command} {first_arg}'.format(cd_to_dir=Constants.CD_TO_PYTHON_DIR,
                                                             python_command=python_command, first_arg=first_arg)


def create_spark_command(parameters, spark_file_to_run, temp_file_path):
    params = defaultdict(str)
    params['spark_file'] = spark_file_to_run
    params['from_date'] = '-from_date ' + parameters.from_date
    params['to_date'] = '-to_date ' + parameters.to_date
    params['hdfs_directory'] = '-d {}'.format(Constants.HDFS_DIR)
    if parameters.searchIoC:
        params['ip_file'] = '-f ' + Constants.IOC_FILE
        params['resource_ip_file'] = '--files ../Resources/' + Constants.IOC_FILE
    elif parameters.ip:
        params['ip_file'] = '-f ' + basename(temp_file_path)
        params['resource_ip_file'] = '--files ../Resources/' + basename(temp_file_path)

    if parameters.ports:
        params['ports'] = ' -p ' + ','.join(parameters.ports)
    return create_command_for_ssh(Constants.SPARK_SUBMIT.format(params))
