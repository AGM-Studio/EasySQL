import EasySQL

# The goal is to have both sync and async be the same as much as we can!
# Check sync example for basic information, here only async related differences are shown!
EasySQL.enable_debug()

# Tables and database should be defined before the main loop starts
database = EasySQL.AsyncDatabase(
    database="MyDatabase",
    password="",
)

# It doesn't matter if you use SQLData or AsyncSQLData, SQLData will create the table based on the type of database.
# But for IDE Compatibility we recommend to use AsyncSQLData for your async project!
class User(EasySQL.AsyncSQLData, database=database, name="MyTable"):
    id = EasySQL.SQLColumn("ID", EasySQL.Types.BIGINT, EasySQL.PRIMARY, EasySQL.AUTO_INCREMENT)
    name = EasySQL.SQLColumn("Name", EasySQL.Types.STRING(255), EasySQL.NOT_NULL, default="Missing!")
    balance = EasySQL.SQLColumn("Balance", EasySQL.Types.BIGINT.UNSIGNED, EasySQL.NOT_NULL) # int default is 0, no need to pass it
    premium = EasySQL.SQLColumn("Premium", EasySQL.Types.BOOL, EasySQL.NOT_NULL, default=False)


# All other tasks should be done inside a main loop!
# Everything is the same as the sync, but we need to await all database requests...
async def main():
    await database.prepare()

    user_1 = User(name="Ashenguard", balance=100, premium=True)
    await User.table.insert(user_1)
    await User.table.insert(name="Bob")

    from random import randint
    for i in range(5):
        await User.table.insert(name=f"User-{i}", balance=randint(0, 20))

    all_data = await User.table.select()
    no_one = await User.table.select(User.name == "NO-ONE")
    one_user = await User.table.select(User.name == "Ashenguard", get_one=True)
    print("Type:", type(one_user))

    mixed = await User.table.select(descending=True, limit=2, order=User.balance, offset=1)
    print(mixed)

    # Where clauses have no ASYNC change!
    eg_and = (User.id != 0) & (User.id < 5)
    eg_or = (User.id != 0) | (User.id < 5)
    eg_not = ~ (User.id > 5)

    EasySQL.WhereIsIn(User.id, [0, 1, 2])
    EasySQL.WhereIsBetween(User.id, 0, 5)
    EasySQL.WhereIsLike(User.name, "Ash%")

    EasySQL.WhereIsIn(User.id, [0, 1, 2]).AND(User.name == "Ash")
    EasySQL.WhereIsEqual(User.id, 5) | EasySQL.WhereIsIn(User.id, [0, 1, 2])
    EasySQL.WhereIsIn(User.id, [0, 1, 2]) | ((User.id != 0) & (User.id < 5))


    one_user.balance += 5
    await User.table.update(one_user)
    await User.table.update_where(User.id == 1, premium=True)
    await User.table.update_where(User.id == 2, User(name="Chosen one!", balance=1000000, premium=True))

    await User.table.delete(User.id > 2)


# Let's run the main loop!
import asyncio

asyncio.run(main())
