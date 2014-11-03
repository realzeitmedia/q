Go queue with file based overflow.

Idea
----

Q is a queue with an upper limit to the number of unprocessed entries which are
kept in memory. If the queue reader starts to lag behind too much, parts of the
queue will be stored on disk. On shutdown the queue also doesn't need to be
completely emptied before termination; the queue will be restored from disk on
the next restart.

If the reader is keeping up the disk won't be touched.


Use case
--------

We use it in HTTP servers which store log events
into ElasticSearch (ES). The ES installation is remote, so sometimes it lags a
little, and sometimes ES is just plain unresponsive or down. This queue shields the HTTP
server from ES and from network hick-ups, and also keeps the requests as quick
as possible.


File format
-----------

Disk files are simple chunks of `gob` encoded lists with parts of the queue in
them. Files have a maximum number of entries so multiple files will be used
when the readers lags behind a lot. The administrator is free to delete and
backup (some of the) files if things really go haywire.


Internals
---------

The queue is chopped in chunks called `batch`es. Once a batch is full it'll be
passed on to the reader. If the reader is too slow the batch will be stored to
disk, otherwise the disk won't be used.

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

When the reader catches up it will open and process the stored batches one by one.


TODO
----

- Handle errors in a better way than just logging them. Maybe an error channel?
- All existing batches are fully read on startup.


&c.
---

[![Build Status](https://travis-ci.org/alicebob/q.svg?branch=master)](https://travis-ci.org/alicebob/q)
