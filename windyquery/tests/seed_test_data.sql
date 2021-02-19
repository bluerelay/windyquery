CREATE TABLE test ("id" serial PRIMARY KEY, "name" VARCHAR(255));
INSERT INTO test ("name") VALUES ('test');

CREATE TABLE cards (id serial PRIMARY KEY, board_id INTEGER, "data" JSONB);
INSERT INTO cards ("board_id", "data") VALUES
    (2, '{ "name": "Wash dishes", "tags":["Clean", "Kitchen"], "finished":false }'::jsonb),
    (7, '{ "name": "Cook lunch", "tags":["Cook", "Kitchen", "Tacos"] }'::jsonb),
    (1, '{}'::jsonb),
    (2, '{ "name": "Hang paintings", "tags":["Improvements", "Office"], "finished":true }'::jsonb),
    (3, '{}'::jsonb),
    (9, '{ "address": { "city": "Chicago" } }'::jsonb),
    (5, '{ "skill": { "java": "good" } }'::jsonb),
    (11, '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::jsonb),
    (21, '{}'::jsonb);


CREATE TABLE users ("id" serial PRIMARY KEY, "email" VARCHAR(255), "password" VARCHAR(255), "admin" BOOLEAN);
INSERT INTO users ("email", "password", "admin") VALUES
    ('test@example.com', 'mypass', 'true'),
    ('test2@example.com', 'mypass2', 'false'),
    ('test3@example.com', 'mypass3', 'false'),
    ('secret@example.com', 'secret', 'false'),
    ('secret@example.com', 'secret', 'false');


CREATE TABLE boards ("id" serial PRIMARY KEY, "user_id" INTEGER, "location" VARCHAR(255));
INSERT INTO boards ("user_id", "location") VALUES
    (1, 'southwest'),
    (2, 'dining room'),
    (3, 'south door');


CREATE TABLE country ("numeric_code" INTEGER PRIMARY KEY, "name" VARCHAR(255), "alpha2" CHAR(2));

CREATE TABLE cards_copy (id INTEGER, board_id INTEGER);

CREATE OR REPLACE FUNCTION cards_after_insert() RETURNS trigger LANGUAGE 'plpgsql' AS
$$
BEGIN
    PERFORM (SELECT pg_notify('cards', 'after insert'));
    RETURN NEW;
END;
$$;

CREATE TRIGGER cards_trigger AFTER INSERT ON cards FOR EACH ROW EXECUTE PROCEDURE cards_after_insert();

CREATE TABLE students ("id" serial PRIMARY KEY, "firstname" TEXT, "lastname" TEXT);

CREATE SCHEMA test1;
