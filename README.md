Scala SQL Interpreter
This project is a Scala-based SQL interpreter that simulates core database functionality. It allows users to define tables,
manage data, and execute SQL-like queries in a simplified environment. The interpreter is modular, with each component
responsible for a key function: table management, data insertion, query execution, and conditional filtering.

The Database module handles table creation and data storage, serving as the backbone for all operations. The Queries
module processes SQL-like commands, allowing users to retrieve data and apply filtering conditions through SQL operations
like SELECT and WHERE. These conditions are managed by the FilterCond module, which applies rules to narrow down data results.
The Table module ensures that all data and structural aspects of tables are handled efficiently, providing a robust
foundation for database interactions.

In summary, this project offers a practical introduction to database interaction, mimicking SQL functionality in Scala.


