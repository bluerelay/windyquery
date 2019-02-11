# windyquery - A non-blocking Python PostgreSQL query builder

Windyquery is a non-blocking PostgreSQL query builder with Asyncio.

### Installation
```
$ pip install windyquery
```

### Connection
```
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

# create DB connection for migration operations
schema = Schema()
asyncio.get_event_loop().run_until_complete(schema.connect('db_name', {
    'host': 'localhost',
    'port': '5432',
    'database': 'db_name',
    'username': 'db_user_name',
    'password': 'db_user_password'
}, default=True, min_size=1, max_size=1))
```

### CRUD operations
```
# SELECT
result = await db.table('users').select().first()
result['name']

# INSERT
await db.table('users').insert(
    {'email': 'test1@example.com', 'password': 'my precious'},
    {'email': 'test2@example.com', 'password': 'my precious'}
)

# UPDATE
await db.table('users').where('id', 2).update({'name': 'new name'})

# DELETE
await db.table('users').where('id', 2).delete()
```