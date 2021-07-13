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
    # SELECT id, name FROM users
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
await db.table('cards').select().where("id", [1, 2])

# SELECT * FROM users WHERE id IN (1, 2)
await db.table('cards').select().where("id", 'IN', [1, 2])

# SELECT * FROM users WHERE id IN (1, 2)
await db.table('cards').select().where("id IN (?, ?)", 1, 2)

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

# INSERT INTO users (id, name) VALUES
#   (1, 'Tom')
#   ON CONFLICT (id) DO NOTHING
await db.table('users').insert(
    {'id': 1, 'name': 'Tom'},
).on_conflict('(id)', 'DO NOTHING')

# INSERT INTO users As u (id, name) VALUES
#   (1, 'Tom')
#   ON CONFLICT ON CONSTRAINT users_pkey
#   DO UPDATE SET name = EXCLUDED.name || ' (formerly ' || u.name || ')'
await db.table('users AS u').insert(
    {'id': 1, 'name': 'Tom'},
).on_conflict(
    'ON CONSTRAINT users_pkey',
    "DO UPDATE SET name = EXCLUDED.name || ' (formerly ' || u.name || ')'"
)        
```

#### UPDATE
```python
# UPDATE cards SET name = 'Tom' WHERE id = 9
await db.table('cards').where('id', 9).update({'name': 'Tom'})

# UPDATE cards SET total = total + 1 WHERE id = 9
await db.table('cards').update('total = total + 1').where('id', 9)

# UPDATE users SET name = 'Tom' WHERE id = 9 RETRUNING *
await db.table('users').update({'name': 'Tom'}).where('id', '=', 9).returning()

# UPDATE users SET name = 'Tom' WHERE id = 9 RETRUNING id, name
await db.table('users').update({'name': 'Tom'}).where('id', '=', 9).returning('id', 'name')

# UPDATE users SET name = orders.name
#   FROM orders
#   WHERE orders.user_id = users.id
await db.table('users').update('name = orders.name').\
    from_table('orders').\
    where('orders.user_id = users.id')

# UPDATE users SET name = products.name, purchase = products.name, is_paid = TRUE
#   FROM orders
#   JOIN products ON orders.product_id = products.id
#   WHERE orders.user_id = users.id
await db.table('users').update('name = product.name, purchase = products.name, is_paid = ?', True).\
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
#    CONSTRAINT unique_email UNIQUE(group_id, email)
#    check(payday > 0 and payday < 8)
#)
await db.schema('TABLE users').create(
    'id            serial PRIMARY KEY',
    'group_id      integer references groups (id) ON DELETE CASCADE',
    'created_at    timestamp not null DEFAULT NOW()',
    'email         text not null unique',
    'is_admin      boolean not null default false',
    'address       jsonb',
    'payday        integer not null',
    'CONSTRAINT unique_email UNIQUE(group_id, email)',
    'check(payday > 0 and payday < 8)',
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

# CREATE UNIQUE INDEX unique_name ON users(name) WHERE soft_deleted = FALSE
await db.schema('UNIQUE INDEX unique_name ON users').create('name',).where('soft_deleted', False)

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
await db.raw("""
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

### Migrations
Windyquery has a preliminary support for database migrations. The provided command-line script is called `wq`. 

#### Generate a migration file
A migration file can be created by,
```bash
# this creates a timestamped migration file, e.g. "20210705233408_create_my_table.py"
$ wq make_migration --name=create_my_table
```

By default, the new file is add to `database/migrations/` under the current working directory. If the diretory does not exist, it will be created first. The file contains an empty function to be filled by the user,
```python
async def run(db):
    # TODO: add code here
    pass
```

Some sample migration templates are provided at [here](https://github.com/bluerelay/windyquery/blob/master/windyquery/scripts/migration_templates.py). They can be automatically inserted in the generated file by specifying the `--template` parameter,
```bash
# the generated file is pre-filled with some code template,
# async def run(db):
#     await db.schema('TABLE my_table').create(
#         'id      serial PRIMARY KEY',
#         'name    text not null unique',
#     )
$ wq make_migration --name=create_my_table --template="create table"

# create a migration file that contains all avaiable templates
$ wq make_migration --name=create_my_table --template=all
```

#### Run migrations
To run all of the outstanding migrations, use the `migrate` sub-command,
```bash
$ wq migrate --host=localhost --port=5432 --database=my-db --username=my-name --password=my-pass

# alternatively, the DB config can be provided by using environment variables
$ DB_HOST=localhost DB_PORT=5432 DB_DATABASE=my-db DB_USERNAME=my-name DB_PASSWORD=my-pass wq migrate
```

#### Use custom directory and database table
The `wq` command requires a directory to save the migration files, and a database table to store executed migrations. By default, the migration directory is `database/migrations/` under the current working directroy, and the database table is named `migrations`. They are created automatically if they do not already exist.
The directory and table name can be customized by using `--migration_dir` and `--migration_table` parameters,
```bash
# creates the migrations file in "my_db_work/migrations/" of the current directory
$ wq make_migration --name=create_my_table --migrations_dir="my_db_work/migrations"

# looks for outstanding migrations in "my_db_work/migrations/" and stores finished migrations in my_migrations table in DB
$ wq migrate --host=localhost --port=5432 --database=my-db --username=my-name --password=my-pass --migrations_dir="my_db_work/migrations" --migrations_table=my_migrations
```

### Syntax checker
A very important part of windyquery is to validate the inputs of the various builder methods. It defines a [Validator](https://github.com/bluerelay/windyquery/blob/master/windyquery/validator/__init__.py) class, which is used to reject input strings not following the proper syntax.
As a result, it can be used separately as a syntax checker for other DB libraries. For example, it is very common for REST API to support filtering or searching parameters specified by the users,
```python
......
# GET /example-api/users?name=Tom&state=AZ;DROP%20TABLE%20Students
url_query = "name=Tom&state=AZ;DROP TABLE Students"
where = url_query.replace("&", " AND ")

from windyquery.validator import Validator
from windyquery.validator import ValidationError
from windyquery.ctx import Ctx

try:
    ctx = Ctx()
    validator = Validator()
    where = validator.validate_where(where, ctx)
except ValidationError:
    abort(400, f'Invalid query parameters: {url_query}')

connection = psycopg2.connect(**dbConfig)
cursor = connection.cursor()
cursor.execute(f'SELECT * FROM users WHERE {where}')
......
```
Please note,
- Except `raw`, all windyquery's own builder methods, such as `select`, `update`, `where`, and so on, already implicitly use these validation functions. They may be useful when used alone, for example, to help other DB libraries validate SQL snippets;
- These validation functions only cover a very small (though commonly used) subset of SQL grammar of Postgres.

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

### RRULE
Windyquery has a rrule function that can "expand" a rrule string into it occurrences (a list of datetimes) by using [dateutil](https://github.com/dateutil/dateutil). A values CTE is prepared from the rrule occurrences, which can be further used by other querries.

#### A simple rrule example
```python
rruleStr = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-04 10:00:00+00:00'::timestamptz),
#   ('2021-03-05 10:00:00+00:00'::timestamptz),
#   ('2021-03-06 10:00:00+00:00'::timestamptz),
#   ('2021-03-07 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr}).table('my_rrules').select()
```

#### More than one rrules
```python
rruleStr1 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

rruleStr2 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;INTERVAL=10;COUNT=3
RRULE:FREQ=DAILY;INTERVAL=5;COUNT=3
"""

# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-04 10:00:00+00:00'::timestamptz),
#   ('2021-03-05 10:00:00+00:00'::timestamptz),
#   ('2021-03-06 10:00:00+00:00'::timestamptz),
#   ('2021-03-07 10:00:00+00:00'::timestamptz),
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-08 10:00:00+00:00'::timestamptz),
#   ('2021-03-13 10:00:00+00:00'::timestamptz),
#   ('2021-03-23 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
)
await db.rrule('my_rrules', {
        'rrule': rruleStr1
    }, {
        'rrule': rruleStr2
    }).table('my_rrules').select()

# the rrule field can also take a list of mulitple rrules.
# the previous example is equivalent to
await db.rrule('my_rrules', {
        'rrule': [rruleStr1, rruleStr2]
    }).table('my_rrules').select()
```

#### Use exrule
```python
rruleStr = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

exruleStr = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;BYWEEKDAY=SA,SU
"""

# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-04 10:00:00+00:00'::timestamptz),
#   ('2021-03-05 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr}).table('my_rrules').select()
```

#### Use rdate
```python
# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-05-03 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rdate': '20210503T100000Z'}).table('my_rrules').select()

rruleStr = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-04 10:00:00+00:00'::timestamptz),
#   ('2021-03-05 10:00:00+00:00'::timestamptz),
#   ('2021-03-06 10:00:00+00:00'::timestamptz),
#   ('2021-03-07 10:00:00+00:00'::timestamptz),
#   ('2021-05-03 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr, 'rdate': '20210503T100000Z'}).table('my_rrules').select()

# similary to rrule, the rdate field can take a list of date strings
# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-04 10:00:00+00:00'::timestamptz),
#   ('2021-03-05 10:00:00+00:00'::timestamptz),
#   ('2021-03-06 10:00:00+00:00'::timestamptz),
#   ('2021-03-07 10:00:00+00:00'::timestamptz),
#   ('2021-05-03 10:00:00+00:00'::timestamptz),
#   ('2021-06-03 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr, 'rdate': ['20210503T100000Z','20210603T100000Z']}).table('my_rrules').select()
```

#### Use exdate
```python
rruleStr = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-05 10:00:00+00:00'::timestamptz),
#   ('2021-03-06 10:00:00+00:00'::timestamptz),
#   ('2021-03-07 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr, 'exdate': '20210304T100000Z'}).table('my_rrules').select()

# similary to rrule, the exdate field can take a list of date strings
# WITH my_rrules ("rrule") AS 
# (VALUES
#   ('2021-03-03 10:00:00+00:00'::timestamptz),
#   ('2021-03-05 10:00:00+00:00'::timestamptz),
#   ('2021-03-07 10:00:00+00:00'::timestamptz)
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr, 'exdate': ['20210304T100000Z','20210306T100000Z']}).table('my_rrules').select()
```

#### Join rrule with other tables
```python
import datetime

rruleStr1 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

rruleStr2 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;INTERVAL=10;COUNT=3
RRULE:FREQ=DAILY;INTERVAL=5;COUNT=3
"""

# WITH task_rrules ("task_id", "rrule") AS 
# (VALUES
#   (1, '2021-03-03 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-04 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-05 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-06 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-07 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-03 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-08 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-13 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-23 10:00:00+00:00'::timestamptz)
# )
# SELECT task_rrules.rrule, tasks.name
# FROM task_rrules
# JOIN tasks ON tasks.id = task_rrules.task_id
# WHERE
#   rrule > '2021-03-05 10:00:00+00:00' AND
#   rrule < '2021-03-08 10:00:00+00:00'
await db.rrule('task_rrules', {
        'task_id': 1, 'rrule': rruleStr1
    }, {
        'task_id': 2, 'rrule': rruleStr2
    }).table('task_rrules').
    join('tasks', 'tasks.id', '=', 'task_rrules.task_id').
    where('rrule > ? AND rrule < ?',
        datetime.datetime(2021, 3, 5, 10, 0,
                tzinfo=datetime.timezone.utc),
        datetime.datetime(2021, 3, 8, 10, 0,
                tzinfo=datetime.timezone.utc),
    ).select('task_rrules.rrule', 'tasks.name')
```

#### Using rrule in update
```python
import datetime

rruleStr1 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

rruleStr2 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;INTERVAL=10;COUNT=3
RRULE:FREQ=DAILY;INTERVAL=5;COUNT=3
"""

# WITH task_rrules ("task_id", "rrule") AS 
# (VALUES
#   (1, '2021-03-03 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-04 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-05 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-06 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-07 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-03 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-08 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-13 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-23 10:00:00+00:00'::timestamptz)
# )
# UPDATE tasks SET result = 'done'
# FROM task_rrules
# WHERE task_rrules.task_id = tasks.id
await db.rrule('task_rrules', {
        'task_id': 1, 'rrule': rruleStr1
    }, {
        'task_id': 2, 'rrule': rruleStr2
    }).table('tasks').update("result = 'done'").
    from_table('task_rrules').
    where('task_rrules.task_id = tasks.id')
```

#### Using rrule with raw method
```python
import datetime

rruleStr1 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

rruleStr2 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;INTERVAL=10;COUNT=3
RRULE:FREQ=DAILY;INTERVAL=5;COUNT=3
"""

# WITH task_rrules ("task_id", "rrule") AS 
# (VALUES
#   (1, '2021-03-03 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-04 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-05 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-06 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-07 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-03 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-08 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-13 10:00:00+00:00'::timestamptz),
#   (2, '2021-03-23 10:00:00+00:00'::timestamptz)
# )
# DELETE FROM tasks
# WHERE EXISTS(
#   SELECT 1 FROM task_rrules
#   WHERE
#     task_id = tasks.id AND
#     rrule > '2021-03-20 10:00:00+00:00'
# )
# RETURNING id, task_id
await db.rrule('task_rrules', {
        'task_id': 1, 'rrule': rruleStr1
    }, {
        'task_id': 3, 'rrule': rruleStr2
    }).raw("""
        DELETE FROM tasks
        WHERE EXISTS(
            SELECT 1 FROM task_rrules
            WHERE 
                task_id = tasks.id AND
                rrule > $1
        )
        RETURNING id, task_id
    """, datetime.datetime(2021, 3, 20, 10, 0,
                tzinfo=datetime.timezone.utc))
```

#### Using a slice to limit the occurrences
```python
import datetime

rruleStr = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY
"""

# WITH my_rrules ("task_id", "rrule") AS 
# (VALUES
#   (1, '2021-03-03 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-04 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-05 10:00:00+00:00'::timestamptz),
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr, 'rrule_slice': slice(3)}).table('my_rrules').select()

# WITH my_rrules ("task_id", "rrule") AS 
# (VALUES
#   (1, '2021-03-13 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-15 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-17 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-19 10:00:00+00:00'::timestamptz),
#   (1, '2021-03-21 10:00:00+00:00'::timestamptz),
# )
# SELECT * FROM my_rrules
await db.rrule('my_rrules', {'rrule': rruleStr, 'rrule_slice': slice(10,20,2)}).table('my_rrules').select()
```


### Tests
Windyquery includes [tests](https://github.com/bluerelay/windyquery/tree/master/windyquery/tests). These tests are also served as examples on how to use this library.

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
