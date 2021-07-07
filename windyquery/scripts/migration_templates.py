templates = {
    'create table': '''
    await db.schema('TABLE IF NOT EXISTS users').create(
        'id            serial PRIMARY KEY',
        'group_id      integer references groups (id) ON DELETE CASCADE',
        'team_id       integer DEFAULT 0',
        'created_at    timestamp not null DEFAULT NOW()',
        'deleted_at    timestamptz DEFAULT NOW()',
        'email         text not null unique',
        'is_admin      boolean not null default false',
        'address       jsonb',
        'num_of_pets   integer CHECK (num_of_pets >= 0) DEFAULT 0',
        'payday        integer not null',
        'salary        smallint CONSTRAINT salarynullable NULL CONSTRAINT salarypositive CHECK (salary > 0)',
        'department_id integer references departments (id) ON DELETE CASCADE',
        'FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE SET DEFAULT',
        'CONSTRAINT unique_email UNIQUE(group_id, email)',
        'UNIQUE(group_id, email)',
        'check(payday > 0 and payday < 8)',
    )
    ''',
    'create table like another table': '''
    await db.schema('TABLE accounts').create(
        'like users'
    )
    ''',
    'alter table': '''
    await db.schema('TABLE users').alter(
        'alter  id TYPE bigint',
        'alter  name SET DEFAULT \\'no_name\\'',
        'alter  COLUMN address DROP DEFAULT',
        'alter  "user info" SET NOT NULL',
        'add    CONSTRAINT check(payday > 1 and payday < 6)',
        'add    UNIQUE(name, email) WITH (fillfactor=70)',
        'add    FOREIGN KEY (group_id) REFERENCES groups (id) ON DELETE SET NULL',
        'drop   CONSTRAINT IF EXISTS idx_email CASCADE',
    )
    ''',
    'add column': '''
    await db.schema('TABLE users').alter('ADD COLUMN address text')
    ''',
    'drop column': '''
    await db.schema('TABLE users').alter('DROP address')
    ''',
    'create index ON table (column1, column2)': '''
    await db.schema('INDEX idx_email ON users').create('name', 'email')
    ''',
    'create unique index ON table (column1) WHERE condition': '''
    await db.schema('UNIQUE INDEX unique_name ON users').create('name',).where('soft_deleted', False)
    ''',
    'drop index': '''
    await db.schema('INDEX idx_email').drop('CASCADE')
    ''',
    'rename table': '''
    await db.schema('TABLE users').alter('RENAME TO accounts')
    ''',
    'rename column': '''
    await db.schema('TABLE users').alter('RENAME email TO email_address')
    ''',
    'rename constraint': '''
    await db.schema('TABLE users').alter('RENAME CONSTRAINT idx_name TO index_name')
    ''',
    'drop table': '''
    await db.schema('TABLE users').drop()
    ''',
    'create table with raw': '''
    await db.raw("""
    CREATE TABLE users(
        id                       INT NOT NULL,
        created_at               DATE NOT NULL,
        first_name               VARCHAR(100) NOT NULL,
        last_name                VARCHAR(100) NOT NULL,
        birthday_mmddyyyy        CHAR(10) NOT NULL,
    )
    """)
    ''',
    'create trigger function': '''
    await db.raw(r"""CREATE OR REPLACE FUNCTION users_changed() RETURNS trigger LANGUAGE 'plpgsql' AS
    $define$
    BEGIN
        PERFORM pg_notify('users', 'changed');
        RETURN NULL;
    END;
    $define$""")
    ''',
    'create trigger': '''
    await db.raw("""
        CREATE TRIGGER users_changed_trigger
        AFTER INSERT OR UPDATE OF name, email, address ON users
        FOR EACH STATEMENT
        EXECUTE PROCEDURE users_changed();
    """)
    ''',
    'drop trigger': '''
    await db.raw("DROP TRIGGER users_changed_trigger ON users")
    ''',
    'drop trigger funtion': '''
    await db.raw("DROP FUNCTION users_changed")
    ''',
    'test_create table': '''
    await db.schema('TABLE test_tmp_users').create(
        'id      serial PRIMARY KEY',
        'name    text not null unique',
    )
    ''',
    'test_drop table': '''
    await db.schema('TABLE test_tmp_users').drop()
    ''',
}
