# KBase Elasticsearch Bulk Writer

This is a server dedicated to bulk-writing elasticsearch index documents.

The server listens to a kafka event stream and builds up index documents into a big queue. It then makes a bulk update request to elasticsearch with the contents of the queue.

All index documents are saved in bulk and serially to avoid any db conflicts.

## Development

Start the servers:

```sh
docker-compose up
```

Run the tests (servers must be running):

```sh
make test
```
