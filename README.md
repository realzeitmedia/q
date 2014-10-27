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

We use it in HTTP servers which get 1K requests/second, and store log events
into ElasticSearch (ES). The ES installation is remote, so sometimes it lags a
little, and sometimes ES is just plain unresponsive. This queue shields the HTTP
server from ES and network hick-ups, while not throwing away events.


File format
-----------

Disk files are simple chunks of `gob` encoded lists with parts of the queue in
them. Files have a maximum number of entries so multiple files will be used
when the readers lags behind a lot. The administrator is free to delete and
backup files if things really go haywire.


Internals
---------

The queue is chopped in chunks called 'batch'es. If the reader is keeping up
both the reader (`Dequeue()`) and the writer (the `Queue` channel) are using
the same `batch` object, and nothing is stored on disk.

When the reader lags behind more than `BlockCount` entries the writer will get
it's own `batch` object. When that one is full it will be written to disk and
writer gets a new `batch` object.

```
[   ##][#####][#####][### ]
    ^   ^      ^        ^
    |   |      |        |
    |   |      |        \-- the writer is fulling this (in memory) batch.
    |   |      |         
    |   |      \-- this batch is not used and is stored on disk.
    |   |
    |   \-- this batch is not used and is stored on disk.
    |
    \-- the reader is reading from this (in memory) batch.
```

When the reader catches up it will process the stored batches one by one, and
eventually switch over the the same batch as the writer is using.


TODO
----

- Optionally limit maximum disk usage.
- Handle errors better than just logging them. Maybe an error channel?
