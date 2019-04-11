# Movies Databases Readme
### Author: JT Mundi
This python program uses a 5000 tuple movies database which is in CSV format and further have nested JSON for some columns. The program connects to the SQL server on the localhost. After connecting all the data is parsed into the database in Second Normal Form. Then built in queries are performed on the database unless specified by the user. 

## Interface
```bash
./program username password <query #>
    Where ’program’ is your python script. ’username’ is the name of the user for the database.
    ’password’ is the password. You can assume that the port and hostname of th edatabase is
    localhost and 3306. The last argument is optional. If a number is specified, then print that
    query. Otherwise, print all queries in the order they are shown here.
```

## Running the program
Cd Into the folder and run the following command.
**NOTE: Run without query # on initial run to ensure data is inserted into the database**
```
python3 movieSQL.py username password <query # (Valid Range 1-5)>
```

**NOTE: The program will only insert data into the schemawhen ran without specific query number Example: python3 ./movieSQL user pass**

## Requirements 
The program is stable and tested for MACOS Mojave 10.14.4. The program have the following environment requirements. 
- Python 3.7 (Default Python bin is set to #!/anaconda3/bin/python/=)
- MYSQL 8.0.14 (On Localhost)

**Libraries**
- csv: Used for parsing csv file.
- json: Used for parsing the json file.
- mysql.connector: Used to establish connection to MYSQL
- prettytable (from_db_cursor): Used to print tables.
- tqdm: Used to show progress bar when inserting data into the database.

## Installation

### Python
https://www.python.org/downloads/

### mysql-connector
https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html
https://pypi.org/project/mysql-connector-python/

Installing mysql-connector using pip
```
pip install mysql-connector-python
```

### prettytable 
https://pypi.org/project/PrettyTable/

Installing prettytable using pip
```
pip install PrettyTable
```

### tqdm 
[![Build Status](https://travis-ci.org/tqdm/tqdm.svg?branch=master)](https://travis-ci.org/tqdm/tqdm)
https://pypi.org/project/tqdm

Installing tqdm using pip
```
pip install tqdm
```


