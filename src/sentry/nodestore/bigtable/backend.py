from __future__ import absolute_import, print_function

import struct

from six import text_type
from google.cloud import bigtable
from simplejson import JSONEncoder, _default_decoder
from django.utils import timezone

from sentry.nodestore.base import NodeStorage
from sentry.utils.cache import memoize

# Cache an instance of the encoder we want to use
json_dumps = JSONEncoder(
    separators=(',', ':'),
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    allow_nan=True,
    indent=None,
    encoding='utf-8',
    default=None,
).encode

json_loads = _default_decoder.decode


class BigtableNodeStorage(NodeStorage):
    """
    A Bigtable-based backend for storing node data.

    >>> BigtableNodeStorage(
    ...     project='some-project',
    ...     instance='sentry',
    ...     table='nodestore',
    ...     default_ttl=timedelta(days=30),
    ... )
    """

    bytes_per_column = 1024 * 1024 * 10
    max_size = 1024 * 1024 * 100
    columns = [text_type(i).encode() for i in range(max_size / bytes_per_column)]
    column_family = b'x'
    ttl_column = b't'

    def __init__(self, project=None, instance='sentry', table='nodestore',
                 automatic_expiry=False, default_ttl=None, **kwargs):
        self.project = project
        self.instance = instance
        self.table = table
        self.options = kwargs
        self.automatic_expiry = automatic_expiry
        self.default_ttl = default_ttl
        super(BigtableNodeStorage, self).__init__()

    @memoize
    def connection(self):
        return (
            bigtable.Client(project=self.project, **self.options)
            .instance(self.instance)
            .table(self.table)
        )

    def delete(self, id):
        row = self.connection.row(id)
        row.delete()
        self.connection.mutate_rows([row])

    def get(self, id):
        row = self.connection.read_row(id)
        if row is None:
            return None
        data = []
        columns = row.cells[self.column_family]

        # Check if a TTL column exists
        # for this row. If there is,
        # we can use the `timestamp` property of the
        # cells to see if we should return the
        # row or not.
        if self.ttl_column in columns:
            has_ttl = True
            now = timezone.now()
            # If we needed the actual value, we could unpack it.
            # ttl = struct.unpack('<I', columns[self.ttl_column][0].value)[0]
        else:
            has_ttl = False

        for column in self.columns:
            try:
                cell = columns[column][0]
            except KeyError:
                break
            else:
                # Check if we're expired
                if has_ttl and cell.timestamp < now:
                    return
                data.append(cell.value)
        return json_loads(b''.join(data))

    def set(self, id, data, ttl=None):
        row = self.connection.row(id)
        # Call to delete is just a state mutation,
        # and in this case is just used to clear all columns
        # so the entire row will be replaced. Otherwise,
        # if an existing row were mutated, and it took up more
        # than one column, it'd be possible to overwrite
        # beginning columns and still retain the end ones.
        row.delete()

        # If we are setting a TTL on this row,
        # we want to set the timestamp of the cells
        # into the future. This allows our GC policy
        # to delete them when the time comes. It also
        # allows us to filter the rows on read if
        # we are past the timestamp to not return.
        # We want to set a ttl column to the ttl
        # value in the future if we wanted to bump the timestamp
        # and rewrite a row with a new ttl.
        ttl = ttl or self.default_ttl
        if ttl is None:
            ts = None
        else:
            ts = timezone.now() + ttl
            row.set_cell(
                self.column_family,
                self.ttl_column,
                struct.pack('<I', int(ttl.total_seconds())),
                timestamp=ts,
            )

        data = json_dumps(data)
        for idx, column in enumerate(self.columns):
            offset = idx * self.bytes_per_column
            chunk = data[offset:offset + self.bytes_per_column]
            if len(chunk) == 0:
                break
            row.set_cell(
                self.column_family,
                column,
                chunk,
                timestamp=ts,
            )
        self.connection.mutate_rows([row])

    def cleanup(self, cutoff_timestamp):
        raise NotImplementedError

    def bootstrap(self):
        table = (
            bigtable.Client(project=self.project, admin=True, **self.options)
            .instance(self.instance)
            .table(self.table)
        )
        if table.exists():
            return

        # With automatic expiry, we set a GC rule to automatically
        # delete rows with an age of 0. This sounds odd, but when
        # we write rows, we write them with a future timestamp as long
        # as a TTL is set during write. By doing this, we are effectively
        # writing rows into the future, and they will be deleted due to TTL
        # when their timestamp is passed.
        if self.automatic_expiry:
            from datetime import timedelta
            gc_rule = bigtable.column_family.MaxAgeGCRule(timedelta(0))
        else:
            gc_rule = None

        table.create(column_families={
            self.column_family: gc_rule,
        })
