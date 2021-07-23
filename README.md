# EasySQL
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
col1 = EasySQL.EasyColumn('ID', EasySQL.INT, primary=True, auto_increment=True)
col2 = EasySQL.EasyColumn('Name', EasySQL.STRING(255), not_null=True, default='Missing')
col3 = EasySQL.EasyColumn('Premium', EasySQL.BOOL, not_null=True)

table = EasySQL.EasyTable(database, 'Users', [col1, col2, col3])

# Insert values with a simple command
table.insert({'Name': 'Ashenguard', 'Premium': True})
table.insert({col2: 'Sam', col3: False})

# Select data with another simple command
# It will return a list of tuples which meet conditions
all = table.select()
premiums = table.select(['ID', 'Name'], EasySQL.WhereIsEqual(col3, True))
specific = table.select(['Name'], where=EasySQL.WhereIsLike(col2, "Ash%").AND(EasySQL.WhereIsLesserEqual(col1, 5)))

# Delete data with a more simpler command
table.delete(EasySQL.WhereIsGreater(col1, 5))

# Update data with following command
table.update({'Premium': True}, EasySQL.WhereIsEqual(col1, 3).OR(EasySQL.WhereIsEqual(col2, 'Sam')))

# Not sure if you should update or insert? Use set and it will be handled
table.set({'ID': 5, 'Name': 'Nathaniel', 'Premium': False}, where=EasySQL.WhereIsEqual(col1, 5))

# Safety error on delete/update/set without a where statement
# table.delete() -> raise EasySQL.DatabaseSafetyException
# Turn the safety off with following command.
database.remove_safety(confirm=True)
# Now there will be no error, it will clean the all data that's why we had safety lock
table.delete()
```