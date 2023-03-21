# sql-data-versioner
The sql-data-versioner is a SQL only framework for PostgreSQL, tested on PostgreSQL 14.6, to support data versioning together with a query store and schema versioning.

As different versioning approaches have different impacts on the performance, our framework supports integrated, separated and hybrid versioning.

As each different versioning approach has there own benefits, the correct versioning approach needs to be evaluated for each database. 



## Usage

First the associated metadata (metainformation.sql) needs to be created. These are if it is the hybrid or separated approach, the history schema **hist**, a view for the primary keys to easier extract them, and a table to keep track of all versioned tables. 

### Versioning

For the versioning, the associated versioning, the {approach}_versioning.sql file needs to be executed, to create the versioning method. 

At the moment automatically all tables of the "data" namespace are versioned, but this can be changed as wished. Also all constraints need to be evaluated and manually adapted, as otherwise constraints can lead to problems, if they interfere with the versioning.

```
CALL add_all_versioning();
```

Adds the versioning to the whole namespace.

For the separated versioning, the file with schema_versioning_woRules.sql supports also the Copy command, whereas the separated_versioning file does not support the Copy command.

### Query Store

In the file querystore_simple.sql in the associated folder, you can find two functions and the tables which need to be created for the querystore.  It per default creates a download and a query store table - which can be altered to the needs of the specific database.

After creation,  the functions are storing a query - these queries need to be rewritten to use the time of the original execution of the query.

### Schema Versioning

For the schema versioning, which is currently only available for the separated versioning, it is necessary to execute all wished versioning functions in the schema_versioning.sql file.

