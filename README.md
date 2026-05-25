# EasySQL - New System 4.0.0
![Downloads](https://pepy.tech/badge/pyeasysql)
![Downloads](https://pepy.tech/badge/pyeasysql/week)
![Downloads](https://pepy.tech/badge/pyeasysql/month)  
This library allow you to run SQL Databases without knowing even SQL.  
This library will create SQL queries and execute them as you request and is very simple to use.

### This library is still under development, so we appreciate if you help us improve it on the GitHub!

### Having an issue?
You can always find someone on our discord server here:
> https://discord.gg/6exsySK

### Wiki
The official wiki of this library is now available at GitHub
> https://github.com/AGM-Studio/EasySQL/wiki

## How to install
To install just use following command
```shell
pip install PyEasySQL
```
This library will have dev/beta builds on the GitHub, to install them you can use

```shell
pip install --upgrade git+https://github.com/AGM-Studio/EasySQL.git
```
***
By installing this library following libraries and their dependencies will be installed too.
```yaml
mysql-connector: Which is the basic library for connecting to database
```
# Example
Link on GitHub: https://github.com/AGM-Studio/EasySQL/blob/master/test.py

```python
import EasySQL.database
import EasySQL.sync.database
import EasySQL

# Enable debug mode if you like to get SPAMMED with SQL!
EasySQL.enable_debug()


# Simply provide connection info to your database
@EasySQL.auto_init
class MyDatabase(EasySQL.database.SyncedDB):
    _database = 'MyDatabase'
    _password = ''
    _host = '127.0.0.1'
    _port = 3306
    _user = 'root'


# Simply create a MyTable with its columns!
@EasySQL.auto_init
class MyTable(EasySQL.EasyTable, database=MyDatabase, name='MyTable'):
    ID = EasySQL.EasyColumn('ID', EasySQL.Types.BIGINT, EasySQL.PRIMARY, EasySQL.AUTO_INCREMENT)
    Name = EasySQL.EasyColumn('Name', EasySQL.Types.STRING(255), EasySQL.NOT_NULL, default='Missing')
    Balance = EasySQL.EasyColumn('Balance', EasySQL.Types.INT, EasySQL.NOT_NULL)
    Premium = EasySQL.EasyColumn('Premium', EasySQL.Types.BOOL, EasySQL.NOT_NULL, default=False)


# Insert values with a simple command
MyTable.insert('Ashenguard', True, 10).into(MyTable.Name, MyTable.Premium, MyTable.Balance).execute()
MyTable.insert('Sam', False).into(MyTable.Name, MyTable.Premium).execute()

# Let's add some random data
from random import randint

for i in range(5):
    MyTable.insert(f'User-{i}', randint(0, 20)).into('Name', 'Balance').execute()

# Selecting data with another simple command
### Let's get all the data
all = MyTable.select().execute()
### Something that does not exist
empty = MyTable.select(MyTable.ID).where(MyTable.Name.is_equal("NO-ONE")).execute()
### To select multiple data give a list of columns as 1st argument
premiums = MyTable.select(MyTable.ID, MyTable.Name).where(MyTable.Premium.is_equal(True)).execute()
### You can have more complicated condition with AND (&), OR (|) and NOT (~)
specific = MyTable.select(MyTable.Name).where(MyTable.Name.is_like("Ash%").AND(MyTable.ID.is_lesser_equal(5))).execute()
### Giving no column will select all the columns, Also you can use limit, offset and order to sort data
second = MyTable.select().order(MyTable.Balance).descending().limit(1).offset(1).execute()
top5 = MyTable.select().order(MyTable.Balance).descending().limit(5).execute()
### If you want only one result, not a sequence of them! It will return a SelectData if a data is found or return None if none is found.
one = MyTable.select().where(MyTable.Name.is_equal("Ashenguard")).just_one().execute()

# The result will be an EmptySelectData if nothing was found, A SelectData if only one was found, Or a tuple of SelectData
# All 3 of them are iterable, so it is safe to use a `for` loop for any result
# To get data from the result you can use `get`, but it only contains columns requested in select method.
for data in all:
    print(data)

for data in top5:
    print(f'{data.get(MyTable.ID)}: {data.get(MyTable.Name)}\tBalance: {data.get(MyTable.Balance)}')

# To delete data just use the delete method
MyTable.delete(MyTable.ID.is_greater(5)).execute()

# Update data with following command
MyTable.update(MyTable.Premium).to(True).where(MyTable.ID.is_equal(3).OR(MyTable.Name.is_equal('Sam'))).execute()

# Safety error on delete/update/set without a where statement
# MyTable.delete() -> raise EasySQL.DatabaseSafetyException
# Turn the safety off with following command.
MyDatabase.remove_safety(confirm=True)
# Now there will be no error, it will clean the all data that's why we had safety lock
MyTable.delete().execute()


# If you want a custom class as your data holder you can use following examples
### Make a subclass of SelectData class
class DataHolderA(EasySQL.SQLData):
    # do as you want but make use of __init__
    def __init__(self, table, data_array, columns):
        super().__init__(table, data_array, columns)


### Add the "from_sql_data" class method to any class which will receive the SQLData
class DataHolderB:
    @classmethod
    def from_sql_data(cls, data: EasySQL.SQLData):
        # Analyze the data the way you like it's up to you!
        return cls(data.get('ID'), data.get('Name'), data.get('Balance'), data.get('Premium'))

    def __init__(self, id, name, balance, premium):
        self.id = id
        self.name = name
        self.balance = balance
        self.premium = premium


# While creating the table class, give the data holder to it
@EasySQL.auto_init
class MyAdvancedTable(EasySQL.EasyTable, database=MyDatabase, name='MyTable', data_class=DataHolderB):
    ID = EasySQL.EasyColumn('ID', EasySQL.Types.BIGINT, EasySQL.PRIMARY, EasySQL.AUTO_INCREMENT)
    Name = EasySQL.EasyColumn('Name', EasySQL.Types.STRING(255), EasySQL.NOT_NULL, default='Missing')
    Balance = EasySQL.EasyColumn('Balance', EasySQL.Types.INT, EasySQL.NOT_NULL)
    Premium = EasySQL.EasyColumn('Premium', EasySQL.Types.BOOL, EasySQL.NOT_NULL, default=False)


MyAdvancedTable.insert(f'Random', randint(0, 20)).into('Name', 'Balance').execute()
random = MyAdvancedTable.select().just_one().execute()
print(random, random.name, random.balance, random.premium)
```

[![Advertisement Banner](https://2captcha.com/referral-banners/2captcha/08.gif)](https://2captcha.com/?from=19092307)

## Extras & Features
1. Need unsigned types? EasySQL has them.
> `BIGINT.UNSIGNED`, `INT.UNSIGNED`, `MEDIUMINT.UNSIGNED`, `SMALLINT.UNSIGNED`
2. Afraid of unsigned or signed values? EasySQL will check them for you!
> Raises `ValueError` if you are out of bound
3. Multiple primary keys? EasySQL will take care of it.
> Tag them with `PRIMARY` or add them to `YourTableClass.PRIMARY`
4. Want to mark multiple columns as unique together? EasySQL have it.
> Add `Unique(column_1, column_2)` to `YourTableClass.UNIQUES`
5. Auto cast data & auto convert to your classes!