Go queue with file based overflow.

Idea
----

Q is a queue with an upper limit to the number of unprocessed entries which are
kept in memory. If the queue reader starts to lag behind too much, parts of the
queue will be stored on disk. On shutdown the queue also doesn't need to be
completely emptied before termination; the queue will be restored from disk on
the next restart.

If the reader is keeping up the disk won't be touched.

The events which are queued will be serialized using `gob` if they need to be
swapped to disk.


Use case
--------

We use it in HTTP servers which store log events
into ElasticSearch (ES). The ES installation is remote, so sometimes it lags a
little, and sometimes ES is just plain unresponsive. This queue shields the HTTP
server from ES and from network hick-ups, while not throwing away events or blowing up the webservers.


File format
-----------

Disk files are simple chunks of `gob` encoded lists with parts of the queue in
them. Files have a maximum number of entries so multiple files will be used
when the readers lags behind a lot. The administrator is free to delete and
backup (some of the) files if things really go haywire.


Internals
---------

The queue is chopped in chunks called `batch`es. If the reader is keeping up
both the reader (`Dequeue()`) and the writer (the `Queue` channel) are using
the same `batch` object, and nothing is stored on disk.

When the reader lags behind more than `BlockCount` entries the writer will get
its own `batch` object. When that one is full it will be written to disk and
a new `batch` object will be filled, et cetera.

```
[   ##][#####][#####][### ]
    ^   ^      ^        ^
    |   |      |        |
    |   |      |        \-- the writer is filling this (in memory) batch.
    |   |      |         
    |   |      \-- this batch is not used and is stored on disk.
    |   |
    |   \-- this batch is not used and is stored on disk.
    |
    \-- the reader is reading from this (in memory) batch.
```

When the reader catches up it will open and process the stored batches one by one, and
eventually switch over to the same batch as the writer is using.


TODO
----

- Handle errors in a better way than just logging them. Maybe an error channel?
- All existing batches are fully read on startup.


&c.
---

[![Build Status](https://travis-ci.org/alicebob/q.svg?branch=master)](https://travis-ci.org/alicebob/q)
