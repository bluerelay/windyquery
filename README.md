# windyquery - A non-blocking Python PostgreSQL query builder

Windyquery is a non-blocking PostgreSQL query builder with Asyncio.

### Installation
```
$ pip install windyquery
```

### Connection
```python
import asyncio

from windyquery import DB, Schema

# create DB connection for CRUD operatons
db = DB()
asyncio.get_event_loop().run_until_complete(db.connect('db_name', {
    'host': 'localhost',
    'port': '5432',
    'database': 'db_name',
    'username': 'db_user_name',
    'password': 'db_user_password'
}, default=True))

asyncio.get_event_loop().run_until_complete(db.connect('other_db_name', {
    'host': 'localhost',
    'port': '5432',
    'database': 'other_db_name',
    'username': 'db_user_name',
    'password': 'db_user_password'
}, default=False))

# create DB connection for migration operations
schema = Schema()
asyncio.get_event_loop().run_until_complete(schema.connect('db_name', {
    'host': 'localhost',
    'port': '5432',
    'database': 'db_name',
    'username': 'db_user_name',
    'password': 'db_user_password'
}, default=True, min_size=1, max_size=1))

# switch connections between different databases
db.connection('other_db_name')

# the default connection can also be dynamically changed
db.default = 'other_db_name'

# close DB connection
asyncio.get_event_loop().run_until_complete(db.stop())
```

### CRUD examples
A DB instance can be used to constuct a SQL. The instance is a coroutine object.
It can be scheduled to run by all [asyncio](https://docs.python.org/3/library/asyncio-task.html) mechanisms.
```python
# SELECT
async def main(db):
    user = await db.table('users').select().first()
    print(user['name'])

asyncio.run(main(db))

# INSERT
await db.table('users').insert(
    {'email': 'test1@example.com', 'password': 'my precious'},
    {'email': 'test2@example.com', 'password': 'my precious'}
)

# UPDATE
await db.table('users').where('id', 2).update({'name': 'new name'})

# DELETE
await db.table('users').where('id', 2).delete()

# Table JOIN
purchases = await db.table('users').join(
    'orders', 'orders.user_id', '=', 'users.id'
).join(
    'products', 'products.id', '=', 'orders.product_id'
).select(
    'users.name AS buyer', 'products.name AS item'
).where("users.id", 2)

# GROUP BY
purchases = await db.table('orders').select('user_name').group_by('user_id')

# ORDER BY
my_orders = await db.table('orders').select().order_by('sub_total DESC', 'user_id')

# LIMIT
users = await db.table('users').select().order_by('name').limit(3)
```

### Migration examples
A Shema instance is for creating or altering tables.
It is commonly used for DB migrations.
```python
# CREATE TABLE
await schema.create('users',
    schema.column('id').serial().primary_key(),
    # schema.column('id').bigserial().primary_key(), # for big tables
    schema.column('email').string().nullable(False).unique(), # default to 256
    schema.column('password').string(32).nullable(False),
    schema.column('message').text().nullable(False),
    schema.column('created_at').timestamp().nullable(False).default("NOW()"),
    schema.column('dinner_time').timestamptz().nullable(False),
    schema.column('is_admin').boolean().nullable(False).default(False),
    schema.column('location_id').integer().nullable(False).index(),
    # schema.column('location_id').bigint().nullable(False).index(),
    schema.column('salary').numeric(8, 2).nullable(False),
    schema.column('settings').jsonb().nullable(False),
)

# RNAME TABLE
await schema.rename('users', 'clients')

# ADD TABLE COLUMN
await schema.table('users',
    schema.column('admin').boolean().nullable(False).default(False)
)

# DROP TABLE COLUMN
await schema.table('users',
    schema.column('admin').drop(),
)

# ADD INDEX
await schema.table('users',
    schema.index('email', 'created_at'),
    schema.unique('location_id'),
)

# DROP INDEX (index is named by table_col1_col2_idx)
await schema.dropIndex('users_email_created_at_idx')

# DROP contraint
await schema.dropConstraint('users', 'unique_key')

# RAW
return schema.raw("""
    CREATE TABLE users(
        id                       INT NOT NULL,
        created_at               DATE NOT NULL,
        first_name               VARCHAR(100) NOT NULL,
        last_name                VARCHAR(100) NOT NULL,
        birthday_mmddyyyy        CHAR(10) NOT NULL,
    )
    """)
```


### JSONB examples
```python
# Create JSONB
await schema.create('users',
    schema.column('id').serial().primary_key(),
    schema.column('data').jsonb(),
)

# Insert JSONB
await db.table('users').insert(
    {'data': {'name': 'user1', 'address': {'city': 'Chicago', 'state': 'IL'}}},
    {'data': {'name': 'user2', 'address': {'city': 'New York', 'state': 'NY'}, 'admin': True}},
)

# SELECT JSONB
user = await db.table('users').select(
    'data->name AS name',
    'data->>name AS name_text',
    'data->address AS address'
).where('id', 2).first() 
# row['name'] == '"user2"'
# row['name_text'] == 'user2'
# row['address'] == '{"name":"user2", "address":{"city":"New York", "state":"NY"}}'

# UPDATE JSONB
await db.table('users').where('id', 2).update({'data': {'address': {'city': 'Richmond'}}})
await db.table('users').where('id', 2).update({'data->address->city': 'Richmond'})

# JSONB in WHERE clause
users = await db.table('users').select('data->>name AS name').where("data->address->city", 'Chicago')
```

### Raw sql examples
Raw sql is needed for creating complex queries.
```python
# select_raw
await db.table('users').select_raw(
    'ROUND(AVG(id),1) AS avg_id, COUNT(1) AS copies'
).where('id', [4,5,6]).first()

# insertRaw
await db.table('users').insertRaw(
    '("id", "name") SELECT $1, $2 WHERE NOT EXISTS (SELECT "id" FROM users WHERE "id" = $1)', [10, 'user name']
))

# order_by_raw
await db.table('users').select().order_by_raw('RANDOM()').limit(10)

# raw
await db.raw('SELECT * FROM users WHERE id = $1', [2]).first()

# use asyncpg (https://magicstack.github.io/asyncpg/current/usage.html)
async with db.conn_pools['db_name'].acquire() as connection:
    await connection.fetchrow('SELECT * FROM test')

```

### Model
To use Model, a **primary key** is required by the underneath table.
By subclassing windyquery.Model calss, you create a model for your table.
By default, the table name should be in the "snake_case" (my_orders),
and the model name is in CamelCase (MyOrder).
Please also note table name has an extra 's' at the end unless it already ends with 's'.
```python
# ==== init_service.py ====
# setup connection
from windyquery import DB
from windyquery.model import Event

model_db = DB()
asyncio.get_event_loop().run_until_complete(model_db.connect('db_name', {
    'host': 'localhost',
    'port': '5432',
    'database': 'db_name',
    'username': 'db_user_name',
    'password': 'db_user_password'
}, default=True))
# make sure this runs after definition of all model classes
Event.db.on_next(model_db)

# ==== models.py ====
from windyquery import Model

# a Model with default setup
class User(Model):
    pass
# User.table == 'users'

# more about naming convention
class AdminUser(Model):
    pass
# AdminUser.table == 'admin_users'

# override table name
class Custom(Model):
    table = 'my_custom'
# Custom.table == 'my_custom'

# find by id
user = await User.find(2)
# user.id == 2

# find mutiple
users = await User.find([1, 2])
# users[1].id == 2

# all
all_users = await User.all()

# find by where
user = await User.where("email", 'test@example.com').first()
users = await User.where("email", 'test@example.com')

# save a new record
user = User(email='test@example.com', password='password')
user = await user.save()

# create a new record if "Tom Smith" is not found
user = await User.where('first_name', 'Tom').where('last_name', 'Smith').first_or_new()

# update existing record
user = await User.find(2)
user.name = 'new name'
await user.save()

# JOSNB is converted to the matching python types (dict, list)
user = await User.find(2)
print(user.data)
# {'data': {'name': 'user2', 'address': {'city': 'New York', 'state': 'NY'}}
user.data['address']['city'] = 'Richmond'
await user.save()
```

### Running tests
Install pytest to run the included tests,
```bash
pip install -U pytest
```

Set up a postgres server with preloaded data. This can be done by using [docker](https://docs.docker.com/install/) with the [official postgre docker image](https://hub.docker.com/_/postgres),
```bash
docker run --rm --name windyquery-test -p 5432:5432 -v ${PWD}/windyquery/tests/seed_test_data.sql:/docker-entrypoint-initdb.d/seed_test_data.sql -e POSTGRES_USER=windyquery-test -e POSTGRES_PASSWORD=windyquery-test -e POSTGRES_DB=windyquery-test -d postgres:12-alpine
```

Note: to use existing postgres server, it must be configured to have the correct user, password, and database needed in tests/conftest.py. Data needed by tests is in tests/seed_test_data.sql.

To run the tests,
```bash
pytest
```
