import os
import time
import re
import glob
import os
import warnings
from pathlib import Path

from windyquery import DB
from .migration_templates import templates


def make_migration(name, template_search, migrations_dir):
    if not name:
        print("name is required - usage: wq make_migration --name=create_my_table")
        return -1

    # search the tempaltes that matches the user search str
    _templates = {}
    if template_search:
        if template_search == 'all':
            _templates = templates
        else:
            _templates = dict(
                filter(lambda item: template_search in item[0],  templates.items()))
    # create the migration dir if not exists
    Path(os.path.join(*migrations_dir)).mkdir(parents=True, exist_ok=True)
    prefix = time.strftime("%Y%m%d%H%M%S")
    name = prefix + "_" + name.replace(".py", "") + ".py"
    full_name = os.path.join(*migrations_dir, name)
    if len(_templates) > 0:
        templateStr = '\n'.join(
            f'    # ------ {desc} ------{code}' for desc, code in _templates.items())
    else:
        templateStr = '    pass'
    with open(full_name, 'w') as fp:
        fp.write(f'async def run(db):\n{templateStr}')
    print("[created] %s" % (full_name))


async def init_db(host, port, database, username, password):
    db = DB()
    await db.connect(database, {
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'password': password,
    }, default=True, min_size=1, max_size=1)
    return db


async def ensure_migrations_table(db, migrations_table):
    return await db.schema(f'TABLE IF NOT EXISTS {migrations_table}').create(
        'id      bigserial PRIMARY KEY',
        'name    text not null',
    )


async def migrate(db, migrations_dir, migrations_table):
    # find the last migration id if it exists
    results = await db.table(migrations_table).select('*').order_by('id DESC').limit(1)
    latest_migration = results[0] if len(results) > 0 else None
    latest_tm = None
    if latest_migration and len(latest_migration) > 0:
        latest_tm = latest_migration['id']

    # find all file names under the migrations_dir
    files = glob.glob(os.path.join(
        *migrations_dir, '20[1-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9]_*.py'))

    # find all haven't been migrated
    mm_re = re.compile(r'.*(\d{14}).*')
    pending_migrations = {}
    for f in files:
        match = mm_re.match(f)
        if(match):
            tm = match.group(1)
            if latest_tm is None or int(tm) > latest_tm:
                pos = f.find(tm)
                mod_name = "."+f[pos:].replace('.py', '')
                pending_migrations[mod_name] = int(tm)
        else:
            warnings.warn('unrecognized migration file {}'.format(f))

    # migrate
    for mod_name in sorted(pending_migrations):
        src = f'{mod_name[1:]}.py'
        with open(os.path.join(*migrations_dir, src), 'r') as fp:
            exec(fp.read())
            await locals()['run'](db)
        await db.table(migrations_table).insert(
            {
                'id': pending_migrations[mod_name],
                'name': mod_name[16:]
            })
        print("[finished] %s" % (mod_name[1:]))

    return "migration finished successfully."
