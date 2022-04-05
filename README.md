# Stratosphere Spark

### Creating a kerberos ticket:
```
1. ssh -p 8000 YOUR_USERNAME@147.32.80.69
2. kinit stratosphere@META
3. Fill in the password for 'stratosphere' account
```

### Executing IP search
```
python execute_ssh.py 
Args:
  + -c ... do NOT copy back files
  + -ip ip1,ip2,ip3 ... list of ips to search separated by commas (or not)
  + -add .... add these ips to IoC file
  + -ioc .... search the IoC file
```
