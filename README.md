# windyquery - A non-blocking Python PostgreSQL query builder

Windyquery is a non-blocking PostgreSQL query builder with Asyncio.

### Installation
```
$ pip install windyquery
```

### Connection
```python
import asyncio

from windyquery import DB

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

# switch connections between different databases
db.connection('other_db_name')

# the default connection can also be changed directly
db.default = 'other_db_name'

# close DB connection
asyncio.get_event_loop().run_until_complete(db.stop())
```

### CRUD examples
A DB instance can be used to constuct a SQL. The instance is a coroutine object.
It can be scheduled to run by all [asyncio](https://docs.python.org/3/library/asyncio-task.html) mechanisms.

#### Build a SQL and execute it
```python
async def main(db):
    # SELECT * FROM users
    users = await db.table('users').select('id', 'name')
    print(users[0]['name'])

asyncio.run(main(db))
```

#### SELECT
```python
# SELECT name AS username, address addr FROM users
await db.table('users').select('name AS username', 'address addr')

# SELECT * FROM users WHERE id = 1 AND name = 'Tom'
await db.table('users').select().where('id', 1).where('name', 'Tom')

# SELECT * FROM users WHERE id = 1 AND name = 'Tom'
await db.table('users').select().where('id', '=', 1).where('name', '=', 'Tom')

# SELECT * FROM users WHERE id = 1 AND name = 'Tom'
await db.table('users').select().where('id = ? AND name = ?', 1, 'Tom')

# SELECT * FROM users WHERE id IN (1, 2)
await db.table('cards').select().where("id", [1, 2]))

# SELECT * FROM users WHERE id IN (1, 2)
await db.table('cards').select().where("id", 'IN', [1, 2]))

# SELECT * FROM users WHERE id IN (1, 2)
await db.table('cards').select().where("id IN (?, ?)", 1, 2))

# SELECT * FROM users ORDER BY id, name DESC
await db.table('users').select().order_by('id', 'name DESC')

# SELECT * FROM users GROUP BY id, name
await db.table('users').select().group_by('id', 'name')

# SELECT * FROM users LIMIT 100 OFFSET 10
await db.table('users').select().limit(100).offset(10)

# SELECT users.*, orders.total FROM users
#   JOIN orders ON orders.user_id = users.id
await db.table('users').select('users.*', 'orders.total').\
    join('orders', 'orders.user_id', '=', 'users.id')
    
# SELECT users.*, orders.total FROM users
#   JOIN orders ON orders.user_id = users.id AND orders.total > 100
await db.table('users').select('users.*', 'orders.total').\
    join('orders', 'orders.user_id = users.id AND orders.total > ?', 100)
```

#### INSERT
```python
# INSERT INTO users(id, name) VALUES
#   (1, 'Tom'),
#   (2, 'Jerry'),
#   (3, DEFAULT)
await db.table('users').insert(
    {'id': 1, 'name': 'Tom'},
    {'id': 2, 'name': 'Jerry'},
    {'id': 3, 'name': 'DEFAULT'}
)

# INSERT INTO users(id, name) VALUES
#   (1, 'Tom'),
#   (2, 'Jerry'),
#   (3, DEFAULT)
#   RETRUNING id, name
await db.table('users').insert(
    {'id': 1, 'name': 'Tom'},
    {'id': 2, 'name': 'Jerry'},
    {'id': 3, 'name': 'DEFAULT'}
).returning('id', 'name')

# INSERT INTO users(id, name) VALUES
#   (1, 'Tom'),
#   (2, 'Jerry'),
#   (3, DEFAULT)
#   RETRUNING *
await db.table('users').insert(
    {'id': 1, 'name': 'Tom'},
    {'id': 2, 'name': 'Jerry'},
    {'id': 3, 'name': 'DEFAULT'}
).returning()
```

#### UPDATE
```python
# UPDATE cards SET name = 'Tom' WHERE id = 9
await db.table('cards').where('id', 9).update({'name': 'Tom'})

# UPDATE users SET name = orders.name
#   FROM orders
#   WHERE orders.user_id = users.id
await db.table('users').update('name = orders.name').\
    from_table('orders').\
    where('orders.user_id = users.id')

# UPDATE users SET name = products.name
#   FROM orders
#   JOIN products ON orders.product_id = products.id
#   WHERE orders.user_id = users.id
await db.table('users').update('name = product.name').\
    from_table('orders').\
    join('products', 'orders.product_id', '=', 'products.id').\
    where('orders.user_id = users.id')
```

#### DELETE
```python
# DELETE FROM users WHERE id = 1
await db.table('users').where('id', 1).delete()

# DELETE FROM users WHERE id = 1 RETURNING id, name
await db.table('users').where('id', 1).delete().returning('id', 'name')
```

### Migration examples
The DB instance can also be used to migrate database schema.

#### CREATE TABLE
```python
# CREATE TABLE users (
#    id            serial PRIMARY KEY,
#    group_id      integer references groups (id) ON DELETE CASCADE,
#    created_at    timestamp not null DEFAULT NOW(),
#    email         text not null unique,
#    is_admin      boolean not null default false,
#    address       jsonb,
#    payday        integer not null,
#    CONSTRAINT check(payday > 0 and payday < 8)
#)
await db.schema('TABLE users').create(
    'id            serial PRIMARY KEY',
    'group_id      integer references groups (id) ON DELETE CASCADE',
    'created_at    timestamp not null DEFAULT NOW()',
    'email         text not null unique',
    'is_admin      boolean not null default false',
    'address       jsonb',
    'payday        integer not null',
    'CONSTRAINT    UNIQUE(group_id, email)',
    'CONSTRAINT check(payday > 0 and payday < 8)',
)

# CREATE TABLE accounts LIKE users
await db.schema('TABLE accounts').create(
    'like users'
)

# CREATE TABLE IF NOT EXISTS accounts LIKE users
await db.schema('TABLE IF NOT EXISTS accounts').create(
    'like users'
)
```

#### Modify TABLE
```python
# ALTER TABLE users
#   ALTER   id TYPE bigint,
#   ALTER   name SET DEFAULT 'no_name',
#   ALTER   COLUMN address DROP DEFAULT,
#   ALTER   "user info" SET NOT NULL,
#   ALTER   CONSTRAINT check(payday > 1 and payday < 6),
#   ADD     UNIQUE(name, email) WITH (fillfactor=70),
#   ADD     FOREIGN KEY (group_id) REFERENCES groups (id) ON DELETE SET NULL,
#   DROP    CONSTRAINT IF EXISTS idx_email CASCADE
await db.schema('TABLE users').alter(
    'alter  id TYPE bigint',
    'alter  name SET DEFAULT \'no_name\'',
    'alter  COLUMN address DROP DEFAULT',
    'alter  "user info" SET NOT NULL',
    'add    CONSTRAINT check(payday > 1 and payday < 6)',
    'add    UNIQUE(name, email) WITH (fillfactor=70)',
    'add    FOREIGN KEY (group_id) REFERENCES groups (id) ON DELETE SET NULL',
    'drop   CONSTRAINT IF EXISTS idx_email CASCADE',
)

# ALTER TABLE users RENAME TO accounts
await db.schema('TABLE users').alter('RENAME TO accounts')

# ALTER TABLE users RENAME email TO email_address
await db.schema('TABLE users').alter('RENAME email TO email_address')

# ALTER TABLE users RENAME CONSTRAINT idx_name TO index_name
await db.schema('TABLE users').alter('RENAME CONSTRAINT idx_name TO index_name')

# ALTER TABLE users ADD COLUMN address text
await db.schema('TABLE users').alter('ADD COLUMN address text')

# ALTER TABLE users DROP address
await db.schema('TABLE users').alter('DROP address')

# CREATE INDEX idx_email ON users (name, email)
await db.schema('INDEX idx_email ON users').create('name', 'email')

# DROP INDEX idx_email CASCADE
await db.schema('INDEX idx_email').drop('CASCADE')

# DROP TABLE users
await db.schema('TABLE users').drop()
```

### Raw
The `raw` method can be used to execute any form of SQL. Usually the `raw` method is used to execute complex hard-coded (versus dynamically built) queries. It's also very common to use `raw` method to run migrations.

The input to `raw` method is not validated, so it is not safe from SQL injection.

#### RAW for complex SQL
```python
await db.raw('SELECT ROUND(AVG(group_id),1) AS avg_id, COUNT(1) AS total_users FROM users WHERE id in ($1, $2, $3)', 4, 5, 6)

await db.raw("SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num, letter)")

await db.raw("""
    INSERT INTO user (id, name)
        SELECT $1, $2 WHERE NOT EXISTS (SELECT id FROM users WHERE id = $1)
""", 1, 'Tom')
```

#### RAW for migration
```python
await schema.raw("""
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
Methods are created to support jsonb data type for some simple use cases.

#### Create a table with jsonb data type
```python
# CREATE TABLE users (
#    id     serial PRIMARY KEY,
#    data   jsonb
#)
await db.schema('TABLE users').create(
    'id     serial PRIMARY KEY',
    'data   jsonb',
)
```

#### Select jsonb field
```python
# SELECT data->name AS name, data->>name AS name_text FROM users
rows = await db.table('users').select('data', 'data->name AS name', 'data->>name AS name_text')
# rows[0]['data'] == '{"name":"Tom"}'
# rows[0]['name'] == '"Tom"'
# rows[0]['name_text'] == 'Tom'

# SELECT data->name AS name FROM users WHERE data->>name LIKE 'Tom%'
await db.table('users').select('data->name AS name').where('data->>name', 'LIKE', 'Tom%')

# SELECT data->name AS name FROM users WHERE data->name = '"Tom"'
await db.table('users').select('data->name AS name').where("data->name", 'Tom')
```

#### Insert jsonb field
```python
# INSERT INTO users (data) VALUES
#   ('{"name": "Tom"}'),
#   ('{"name": "Jerry"}')
#   RETURNING *
await db.table('users').insert(
    {'data': {'name': 'Tom'}},
    {'data': {'name': 'Jerry'}},
).returning()
```

#### Update jsonb field
```python
# UPDATE SET data = '{"address": {"city": "New York"}}'
await db.table('users').update({'data': {'address': {'city': 'New York'}}})

# UPDATE SET data = jsonb_set(data, '{address,city}', '"Chicago"')
await db.table('users').update({'data->address->city': 'Chicago'})
```


### Listen for a notification
Postgres implements [LISTEN/NOTIFY](https://www.postgresql.org/docs/12/sql-listen.html) for interprocess communications.
In order to listen on a channel, use the DB.listen() method. It returns an awaitable object, which resolves to a dict when a notification fires.
```python
# method 1: manually call start() and stop()
listener = db.listen('my_table')
await listener.start()
try:
    for _ in range(100):
        result = await listener
        # or result = await listener.next()
        print(result) 
        # {
        #     'channel': 'my_table',
        #     'payload': 'payload fired by the notifier',
        #     'listener_pid': 7321,
        #     'notifier_pid': 7322
        # }
finally:
    await listener.stop()

# method 2: use with statement
async with db.listen('my_table') as listener:
    for _ in range(100):
        result = await listener
        print(result)
```

### Tests
Windyquery includes tests [LISTEN/NOTIFY](https://github.com/bluerelay/windyquery/tree/master/windyquery/tests). These tests are also good examples on how to use this library.

#### Running tests
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
