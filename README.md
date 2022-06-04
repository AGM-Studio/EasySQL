# EasySQL
![Downloads](https://pepy.tech/badge/pyeasysql)
![Downloads](https://pepy.tech/badge/pyeasysql/week)
![Downloads](https://pepy.tech/badge/pyeasysql/month)  
This library allow you to run SQL Databases without knowing even SQL.  
This library will create SQL queries and execute them as you request and is very simple to use.

### Support
You can find support on our discord server here:
> https://discord.gg/6exsySK  
> Pay us a visit there ✌

### Wiki
The official wiki of this library is now available at Github
> https://github.com/Ashengaurd/EasySQL/wiki


## How to install
![](https://img.shields.io/github/v/release/Ashengaurd/EasySQL?label=Release&logo=github&style=plastic)
![](https://img.shields.io/github/last-commit/Ashengaurd/EasySQL/master?label=Date&logo=git&logoColor=blue&style=plastic)  
![](https://img.shields.io/github/v/release/Ashengaurd/EasySQL?include_prereleases&label=Development&logo=github&style=plastic)
![](https://img.shields.io/github/last-commit/Ashengaurd/EasySQL?label=Date&logo=git&logoColor=red&style=plastic)  
To install just use following command
```shell
pip install PyEasySQL
```
This library will have dev/beta builds on the github, to install them you can use

```shell
pip install --upgrade git+https://github.com/Ashengaurd/EasySQL.git
```
***
By installing this library following libraries and their dependencies will be installed too.
> mysql-connector: Which is the basic library for connecting to database

# Example

```python
import EasySQL

# Define database which will be needed by any table you create.
database = EasySQL.EasyDatabase(host='127.0.0.1', port=3306,
                                database='DatabaseName',
                                user='username', password='PASSWORD')

# Define tables and columns
ID = EasySQL.EasyColumn('ID', EasySQL.INT, primary=True, auto_increment=True)
Name = EasySQL.EasyColumn('Name', EasySQL.STRING(255), not_null=True, default='Missing')
Balance = EasySQL.EasyColumn('Balance', EasySQL.INT, not_null=True)
Premium = EasySQL.EasyColumn('Premium', EasySQL.BOOL, not_null=True, default=False)

table = EasySQL.EasyTable(database, 'Users', [ID, Name, Balance, Premium])

# Insert values with a simple command
table.insert([Name, Premium, Balance], ['Ashenguard', True, 10])
table.insert([Name, Premium], ['Sam', False])

# Some random data
from random import randint
for i in range(5):
    table.insert([Name, Balance], [f'User-{i}', randint(0, 20)])

# Selecting data with another simple command
### Get all the data
all = table.select()
### Something that does not exist
empty = table.select(ID, where=EasySQL.WhereIsEqual(Name, "NO-ONE"))
### To select multiple data give a list of columns as 1st argument
premiums = table.select([ID, Name], EasySQL.WhereIsEqual(Premium, True))
### You can have more complicated condition with AND (&), OR (|) and NOT (~)
specific = table.select(Name, where=EasySQL.WhereIsLike(Name, "Ash%").AND(EasySQL.WhereIsLesserEqual(ID, 5)))
### Giving no column will select all the columns, Also you can use limit, offset and order sorting data
second = table.select(order=Balance, descending=True, limit=1, offset=1)
top5 = table.select(order=Balance, descending=True, limit=5)

# The result will be an EmptySelectData if nothing was found, A SelectData if only one was found, Or a tuple of SelectData
# All 3 of them are iterable, so it is safe to use a `for` loop for any result
# To get data from the result you can use `get`, but it only contains columns requested in select method.
for data in top5:
    print(f'{data.get(ID)}: {data.get(Name)}\tBalance: {data.get(Balance)}')

# To delete data just use the delete method
table.delete(EasySQL.WhereIsGreater(ID, 5))

# Update data with following command
table.update(Premium, True, EasySQL.WhereIsEqual(ID, 3).OR(EasySQL.WhereIsEqual(Name, 'Sam')))

# Not sure if you should update or insert? Use set and it will be handled
table.set([ID, Name, Balance, Premium], [5, 'Nathaniel', 50, False], where=EasySQL.WhereIsEqual(ID, 5))

# Safety error on delete/update/set without a where statement
# table.delete() -> raise EasySQL.DatabaseSafetyException
# Turn the safety off with following command.
database.remove_safety(confirm=True)
# Now there will be no error, it will clean the all data that's why we had safety lock
table.delete()
```

