PostgreSQL:

* run repl: psl jom -U jom
* init: refer to db.py
* run temporarily: ``postgres -D /usr/local/var/postgres``
* create db: ``cat jom.sql | psql jom -U jom``
* drop table (in REPL): ``drop table xxx;``

How to use:

1. REPL: ``python -m jom.repl``
2. Polling : ``python -m jom.polling``
...
