Zedstore - Column Store for PostgreSQL
--------------------------------------

Zedstore is a column store for PostgreSQL. It is under development. The development happens here, you are in the right place.

This is a fork of the PostgreSQL repository. All the interesting Zedstore stuff is in the src/backend/access/zedstore/ subdirectory. There are only few modifications to the rest of PostgreSQL, outside that subdirectory. Eventually, any modifications needed to accommodate Zedstore needs to be submitted as separate patches and committed to PostgreSQL itself.

Join the discussion on pgsql-hackers:

https://www.postgresql.org/message-id/CALfoeiuF-m5jg51mJUPm5GN8u396o5sA2AF5N97vTRAEDYac7w@mail.gmail.com


Try it out
----------

Clone the repository, and compile it. Use the --with-lz4 option configure option, otherwise zedstore will be horrendously slow:

    ./configure --with-lz4

To use zedstore:

    CREATE TABLE mytable (t text) using zedstore;

Or you can set it as the default for all your tables, in postgresql.conf:

    default_table_access_method = 'zedstore'

