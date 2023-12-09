# TOBS DB - a database by Tobs

## DISCLAIMER

I know absolutely nothing about database design or anything like that.
This is just a fun little project I decided to start because I was a little bored and ended up committing to.

If you see anything that doesn't make sense, feel free to slighlty roast me and/or hmu about how it should be done.

## Overview

This is the database server logic, I'll probably also build some client side API libraries for interacting with this in the future.

In general the project has been fun and I've learned so much about golang (the language it is written in) and database design - which is the actual objective here, LEARNING.

If this turns out to be a super cool project, used by millions, let it be known that I always believed in myself and remember... "NEVER BACK DOWN, NEVER WHAT?"

## TODO:

- Work on the docs (Help wanted!!!)
- Transactions (could do map of [transation_id] -> [db_data] and the client has to include the transaction id to execute an operation on the transaction)
- Move from JSON to BSON (or some other format, maybe protobuf???)
- Work on Golang client
- Work on tdb-cli
