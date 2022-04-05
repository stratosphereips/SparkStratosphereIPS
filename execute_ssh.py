__author__ = 'username'

import getpass
import os
import sys
from os.path import basename
import Utils
import argument_parser as ap

parser = ap.parser
parameters = parser.parse_args(sys.argv[1:])

username = Utils.get_username()

ssh = Utils.get_ssh_connection(username)

if not Utils.is_authorised(ssh, parameters.debug):
    print('You have to create a kerberos ticket to run the job.')
    metacentrum_password = getpass.getpass('Stratosphere@metacentrum password: ')
    exit_status = Utils.create_krb_ticket(ssh, metacentrum_password)
    del metacentrum_password
    if exit_status == 0:
        print('Kerberos ticket has been created.')
    else:
        raise Exception(
            'You have to create kerberos ticket manually. For more info see '
            'www.github.com/ykasimov/SparkStratosphereIPS')

if Utils.is_cluster_busy(ssh):
    print('Cluster is currently busy with another task. Try in 5 minutes.')
    exit(1)

command = Utils.create_command_for_ssh('python3 -c \'import Remote_Utils; Remote_Utils.clean_up_after('
                                       ')\'')
_, ssh_stdout, _ = ssh.exec_command(command)

spark_file_to_run = parameters.spark_file

# if user passes -ip we create a temporary file with this ips and pass it to spark to search.
temp_file_path = ''
if parameters.ip is not None:
    should_be_removed, temp_file_path = Utils.create_temp_file(parameters.ip)
    Utils.upload_tmp_file_to_remote(ssh, temp_file_path)
    if should_be_removed:
        os.unlink(temp_file_path)

# if -add parameter is passed append ips from this file to the IoC file
if parameters.addIPsToIoC and parameters.ip:
    command = Utils.create_command_for_ssh('python3 -c \'import Remote_Utils; Remote_Utils.add_from_temp_to_IoC()\'',
                                           first_arg=basename(temp_file_path))
    _, ssh_stdout, std_err = ssh.exec_command(command)
    exit_status = ssh_stdout.channel.recv_exit_status()
    if exit_status == 0:
        print("IPs are successfully added to the IoC file")
    else:
        print(std_err.read())

# clean Results directory on the remote machine
command = Utils.create_command_for_ssh('python3 -c \'import Remote_Utils; Remote_Utils.clean_results_before()\'')
_, ssh_stdout, std_err = ssh.exec_command(command)
exit_status = ssh_stdout.channel.recv_exit_status()
if exit_status == 0:
    print("Remote Result directory is cleaned")
else:
    print(std_err.read())

# Run Spark job
print('Starting Spark job')
spark_command = Utils.create_spark_command(parameters, spark_file_to_run, temp_file_path)
_, ssh_stdout, std_err = ssh.exec_command(spark_command)
exit_status = ssh_stdout.channel.recv_exit_status()

if exit_status == 0:
    print("Spark job finished")
else:
    print("Error", ''.join(std_err.readlines()))

# Copy files from an hdfs to a edge node dir
command = Utils.create_command_for_ssh('python3 -c \'import Remote_Utils; Remote_Utils.copy_files()\'')
_, ssh_stdout, std_err = ssh.exec_command(command)

exit_status = ssh_stdout.channel.recv_exit_status()

if parameters.decrypt:
    command = Utils.create_command_for_ssh('python3 -c \'import Remote_Utils; Remote_Utils.decrypt_results()\'')
    _, ssh_stdout, std_err = ssh.exec_command(command)
    exit_status = ssh_stdout.channel.recv_exit_status()

if parameters.shouldCopyFilesBack:
    Utils.get_files_from_remote(ssh, username)
    print("Files are copied")

# delete an output hdfs dir
command = Utils.create_command_for_ssh('python3 -c \'import Remote_Utils; Remote_Utils.clean_up_after('
                                       ')\'')
_, ssh_stdout, _ = ssh.exec_command(command)
ssh.close()
