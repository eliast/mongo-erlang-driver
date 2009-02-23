-define(ZERO, <<0:8>>).

-define(TIMEOUT, 100).
-define(TCP_OPTS, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, true}
]).
-define(MD_DEFAULT_HOST, "localhost").
-define(MD_DEFAULT_PORT, 27017).

-record(cursor, {pool, skip=0, limit=0, retrieved=0, collection, spec, fields, id, cid, documents=[], killed=false}).
