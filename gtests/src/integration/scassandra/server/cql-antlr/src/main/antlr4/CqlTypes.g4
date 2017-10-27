grammar CqlTypes;

options {
    language = Java;
}

data_type
    : native_type
    | list_type
    | set_type
    | map_type
    | tuple_type
    ;


native_type
    : 'ascii'
    | 'bigint'
    | 'blob'
    | 'boolean'
    | 'counter'
    | 'date'
    | 'decimal'
    | 'double'
    | 'float'
    | 'inet'
    | 'int'
    | 'smallint'
    | 'text'
    | 'time'
    | 'timestamp'
    | 'timeuuid'
    | 'tinyint'
    | 'uuid'
    | 'varchar'
    | 'varint'
    ;

list_type
    : 'list' '<' data_type '>'
    ;

set_type
    : 'set' '<' data_type '>'
    ;

map_type
    : 'map' '<' data_type ',' data_type '>'
    ;

tuple_type
    : 'tuple' '<' (data_type ',') + data_type '>'
    ;