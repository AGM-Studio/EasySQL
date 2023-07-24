# EasySQL
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

```python
import EasySQL

# Simply provide connection info to your database
@EasySQL.auto_init
class MyDatabase(EasySQL.EasyDatabase):
    _database = 'MyDatabase'
    _password = '**********'
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
MyTable.insert([MyTable.Name, MyTable.Premium, MyTable.Balance], ['Ashenguard', True, 10])
MyTable.insert([MyTable.Name, MyTable.Premium], ['Sam', False])

# Let's add some random data 
from random import randint
for i in range(5):
    MyTable.insert(['Name', 'Balance'], [f'User-{i}', randint(0, 20)])

# Selecting data with another simple command
### Let's get all the data
all = MyTable.select()
### Something that does not exist
empty = MyTable.select(MyTable.ID, where=EasySQL.WhereIsEqual(MyTable.Name, "NO-ONE"))
### To select multiple data give a list of columns as 1st argument
premiums = MyTable.select([MyTable.ID, MyTable.Name], EasySQL.WhereIsEqual(MyTable.Premium, True))
### You can have more complicated condition with AND (&), OR (|) and NOT (~)
specific = MyTable.select(MyTable.Name, where=EasySQL.WhereIsLike(MyTable.Name, "Ash%").AND(EasySQL.WhereIsLesserEqual(MyTable.ID, 5)))
### Giving no column will select all the columns, Also you can use limit, offset and order to sort data
second = MyTable.select(order=MyTable.Balance, descending=True, limit=1, offset=1)
top5 = MyTable.select(order=MyTable.Balance, descending=True, limit=5)
### If you want only one result not a sequence of them! It will return a SelectData if a data is found or return None if none is found.
one = MyTable.select(where=EasySQL.WhereIsEqual(MyTable.Name, "Ashenguard"), force_one=True)

# The result will be an EmptySelectData if nothing was found, A SelectData if only one was found, Or a tuple of SelectData
# All 3 of them are iterable, so it is safe to use a `for` loop for any result
# To get data from the result you can use `get`, but it only contains columns requested in select method.
for data in top5:
    print(f'{data.get(MyTable.ID)}: {data.get(MyTable.Name)}\tBalance: {data.get(MyTable.Balance)}')

# To delete data just use the delete method
MyTable.delete(EasySQL.WhereIsGreater(MyTable.ID, 5))

# Update data with following command
MyTable.update(MyTable.Premium, True, EasySQL.WhereIsEqual(MyTable.ID, 3).OR(EasySQL.WhereIsEqual(MyTable.Name, 'Sam')))

# Not sure if you should update or insert and don't have primary or unique keys? Use set and it will be handled
MyTable.set(MyTable.columns, [6, 'Nathaniel', 50, False], where=EasySQL.WhereIsEqual(MyTable.ID, 5))

# And if you have unique and primary keys set let insert handle duplicates for you
MyTable.insert(MyTable.columns, [3, 'Jack', 5000, False], update_on_dup=True)

# Safety error on delete/update/set without a where statement
# MyTable.delete() -> raise EasySQL.DatabaseSafetyException
# Turn the safety off with following command.
MyDatabase.remove_safety(confirm=True)
# Now there will be no error, it will clean the all data that's why we had safety lock
MyTable.delete()
```

[![AdFoc.us Banner](https://adfoc.us/images/banners/728x90-2.gif)](https://adfoc.us/?refid=497244)

## Extras & Features
1. Need unsigned types? EasySQL has them.
> `BIGINT.UNSIGNED`, `INT.UNSIGNED`, `MEDIUMINT.UNSIGNED`, `SMALLINT.UNSIGNED`
2. Afraid of unsigned or signed values? EasySQL will check them for you!
> Raises `ValueError` if you are out of bound
3. Multiple primary keys? EasySQL will take care of it.
> Tag them with `PRIMARY` or add them to `YourTableClass.PRIMARY`
4. Want to mark multiple columns as unique together? EasySQL have it.
> Add `Unique(column_1, column_2)` to `YourTableClass.UNIQUES`
5. 