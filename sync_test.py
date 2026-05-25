import EasySQL

# Enable debug mode if you like to get SPAMMED with SQL!
EasySQL.enable_debug()

# Simply provide connection info to your database
database = EasySQL.SyncedDatabase(
    database="MyDatabase",
    password="", # host, port and user set by default to usual localhost settings
)

# In V5 the table is now merged with table! Access the table through User.table!
class User(EasySQL.SQLData, database=database, name="MyTable"):
    id = EasySQL.SQLColumn("ID", EasySQL.Types.BIGINT, EasySQL.PRIMARY, EasySQL.AUTO_INCREMENT)
    name = EasySQL.SQLColumn("Name", EasySQL.Types.STRING(255), EasySQL.NOT_NULL, default="Missing!")
    balance = EasySQL.SQLColumn("Balance", EasySQL.Types.BIGINT.UNSIGNED, EasySQL.NOT_NULL) # int default is 0, no need to pass it
    premium = EasySQL.SQLColumn("Premium", EasySQL.Types.BOOL, EasySQL.NOT_NULL, default=False)

# Make sure to call database prepare once after a table is defined.
# No need to do it per table, just one after all tables is enough.
database.prepare()

# Insert values with a simple command giving the data object!
user_1 = User(name="Ashenguard", balance=100, premium=True) # Uses the default for any field mising!
User.table.insert(user_1)       # Give a premade object...
User.table.insert(name="Bob")   # Or let the method creates it!

# Let's  also add some random data
from random import randint
for i in range(5):
    User.table.insert(name=f"User-{i}", balance=randint(0, 20))

# Selecting data with another simple command which returns a list of data fetched.
### Let's get all the data
all_data = User.table.select()
no_one = User.table.select(User.name == "NO-ONE") # This is Still a list
mid_class = User.table.select(EasySQL.WhereIsBetween(User.balance, 50, 150))
print(all_data, no_one, mid_class)
### If you want only one object, then get one! Will return a User or None
one_user = User.table.select(User.name == "Ashenguard", get_one=True)
print("Type:", type(one_user))  # Prints User!
### You can also define order, descending, limit and offset too!
mixed = User.table.select(descending=True, limit=2, order=User.balance, offset=1)
print(mixed)

# Advanced Where clauses!
### While ==, !=, <, >, <=, >= work for common where clauses you can have them chained by binary operators
eg_and = (User.id != 0) & (User.id < 5)
eg_or = (User.id != 0) | (User.id < 5)
eg_not = ~ (User.id > 5)
### You also have access to more advanced ones like:
EasySQL.WhereIsIn(User.id, [0, 1, 2])
EasySQL.WhereIsBetween(User.id, 0, 5)
EasySQL.WhereIsLike(User.name, "Ash%")
### You can chain them with binary operators with general Where clauses or use AND, OR, NOT!
EasySQL.WhereIsIn(User.id, [0, 1, 2]).AND(User.name == "Ash")
EasySQL.WhereIsEqual(User.id, 5) | EasySQL.WhereIsIn(User.id, [0, 1, 2])
EasySQL.WhereIsIn(User.id, [0, 1, 2]) | ((User.id != 0) & (User.id < 5))

# Finally to update data you have also 2 approaches!
### A. Let the object auto updates itself:
### This approach will generate the Where clause by PRIMARY tags. Which means you can't change a primary value.
one_user.balance += 5
User.table.update(one_user) # The Where clause will be as if you've done: "User.id == one_user.id"
### B. Give the where clause yourself!
### It's useful if you have no primary key in your table, or you want to change a primary value!
User.table.update_where(User.id == 1, premium=True)
### You can also use the insert pattern and pass a whole new object, and it will update it whole!
User.table.update_where(User.id == 2, User(name="Chosen one!", balance=1000000, premium=True))

# Delete data with simple commands again!
User.table.delete(User.id > 2)