#!/usr/bin/env bash
cat $* |
grep -a -E "^(Broken|Syntax|Error) " |
# With testdrive, to investigate
grep -v ", found operator" | # Syntax, fix
grep -v ", found identifier" | # Syntax, fix
grep -v "multiple timelines within one dataflow are not supported" | # Happens very often, figure out how to avoid
#grep -v "avro deserialization error" | # Expected with public.resolution_int2double, could exclude td file that creates it
#grep -v "protobuf deserialization error" | # Expected with public.total_garbage, could exclude td file that creates it
#grep -v "Envelope error: Flat: Value not present for message" | # Expected with public.missing_keys_or_values, could exclude td file that creates it
# Expected AFTER a crash, the query before this is interesting, not the ones after
grep -v "Broken 08000: no connection to the server" |
grep -v "failed: Connection refused" |

grep -v "canceling statement due to statement timeout" | # expected in deletes
grep -v "numeric field overflow" |
grep -v "violates not-null constraint" |
grep -v "division by zero" |
grep -v "zero raised to a negative power is undefined" |
grep -v "cannot return complex numbers" |
grep -v "unrecognized privilege type" |
grep -v "statement batch size cannot exceed" |
grep -v "operator does not exist" | # For list types
grep -v "does not exist" | # For roles
# Refinement:
grep -v "value too long for type" |
grep -v "list_agg on char not yet supported" |
grep -v "does not allow subqueries" |
grep -v "range constructor flags argument must not be null" |
grep -v "function array_remove(" |
grep -v "function array_cat(" |
grep -v "function array_position(" |
grep -v "function array_string(" |
grep -v "function array_to_string(" |
grep -v "function array_length(" |
grep -v "function array_upper(" |
grep -v "function upper_inc(" |
grep -v "function map_length(" |
grep -v "function isempty(" |
grep -v "function list_append(" |
grep -v "function list_cat(" |
grep -v "function list_prepend(" |
grep -v "function list_length(" |
grep -v "function lower_inf(" |
grep -v "function upper_inf(" |
grep -v "function lower_inc(" |
grep -v "function upper_inc(" |
grep -v "array_fill on uint8 list not yet supported" |
grep -v "does not support implicitly casting from" |
grep -v "aggregate functions that refer exclusively to outer columns not yet supported" | # https://github.com/MaterializeInc/materialize/issues/3720
grep -v "range lower bound must be less than or equal to range upper bound" |
grep -v "length must be nonnegative" |
grep -v "is only defined for finite arguments" |
grep -v "more than one record produced in subquery" |
grep -v "invalid range bound flags" |
grep -v "invalid input syntax for type jsonb" |
grep -v "invalid regular expression" |
grep -v "invalid input syntax for type date" |
grep -v "invalid escape string" |
grep -v "invalid hash algorithm" |
grep -v "is defined for numbers greater than or equal to" |
grep -v "is not defined for zero" |
grep -v "is not defined for negative numbers" |
grep -v "requested character too large for encoding" |
grep -v "internal error: unrecognized configuration parameter" |
grep -v "invalid encoding name" |
grep -v "invalid time zone" |
grep -v "interval out of range" |
grep -v "uint2 out of range" |
grep -v "uint4 out of range" |
grep -v "uint8 out of range" |
grep -v "out of valid range" |
grep -v "value out of range: overflow" |
grep -v "value out of range: underflow" |
grep -v "LIKE pattern exceeds maximum length" |
grep -v "negative substring length not allowed" |
grep -v "cannot take square root of a negative number" |
grep -v "timestamp units not yet supported" |
grep -v "step size cannot equal zero" |
grep -v "stride must be greater than zero" |
grep -v "timestamp out of range" |
grep -v "integer out of range" |
grep -v "bigint out of range" |
grep -v "smallint out of range" |
grep -v "unterminated escape sequence in LIKE" |
grep -v "null character not permitted" |
grep -v "is defined for numbers between" |
grep -v "field position must be greater than zero" |
grep -v "must appear in the GROUP BY clause or be used in an aggregate function" |
grep -v "pg_cancel_backend in this position not yet supported" |
#grep -v "internal error: role " |
#grep -v "internal error: schema " |
#grep -v "internal error: object " |
#grep -v "internal error: database " |
#grep -v "internal error: global ID " |
grep -v "invalid datepart" |
grep -v "string is not a valid identifier" |
grep -v "could not determine polymorphic type" |
grep -v "dimension array or low bound array must not be null" |
grep -v "initial position must not be null" |
grep -v "timezone interval must not contain months or years" |
grep -v "unrecognized configuration parameter" |
grep -v "array_fill on " |
grep -v "mz_aclitem grantor cannot be PUBLIC role" |
grep -v "is out of range for type numeric: exceeds maximum precision" |
grep -v "requested length too large" |

grep -v "operator is not unique" | # random nulls
grep -v "cannot be matched" | # random nulls
grep -v "CAST does not support casting from" | # random nulls

# Functions returning sets:
grep -v "number of columns must be a positive integer literal" |
grep -v "regex_extract requires a string literal as its first argument" |
grep -v -E "column \".*\" does not exist" | # https://github.com/MaterializeInc/materialize/issues/20533
grep -v "SET clause does not support casting from interval to integer" |

grep -v "Expected joined table, found" | # Should fix for multi table join
grep -v "Expected ON, or USING after JOIN, found" | # Should fix for multi table join
grep -v "but expression is of type" | # Should fix, but only happens rarely
grep -v "coalesce could not convert type map" | # Should fix, but only happens rarely
grep -v "operator does not exist: map" | # Should fix, but only happens rarely
grep -v "result exceeds max size of" | # Seems expected with huge queries
grep -v "expected expression, but found reserved keyword" | # Should fix, but only happens rarely with subqueries
grep -v "Expected right parenthesis, found left parenthesis" | # Should fix, but only happens rarely with cast+coalesce
grep -v "coalesce could not convert type character" | # https://github.com/MaterializeInc/materialize/issues/17899
grep -v "coalesce could not convert type \"char\"" | # https://github.com/MaterializeInc/materialize/issues/17899
grep -v "invalid selection: operation may only refer to user-defined tables" | # Seems expected
grep -v "cannot reference pseudo type" | # https://github.com/MaterializeInc/materialize/issues/17870
grep -v "Invalid data in source, saw retractions" | # https://github.com/MaterializeInc/materialize/issues/17874
#grep -v "internal error: unimplemented join" | # https://github.com/MaterializeInc/materialize/issues/17897
grep -v "' not recognized" | # https://github.com/MaterializeInc/materialize/issues/17981
grep -v "internal error: Invalid data in source, saw negative accumulation for key" | # https://github.com/MaterializeInc/materialize/issues/17509
grep -v "internal transform error: scalar types do not match" | # https://github.com/MaterializeInc/materialize/issues/18023
grep -v "array_agg on arrays not yet supported" | # https://github.com/MaterializeInc/materialize/issues/18044
grep -v "Unsupported temporal predicate." | # https://github.com/MaterializeInc/materialize/issues/18048
grep -v "OneShot plan has temporal constraints" | # https://github.com/MaterializeInc/materialize/issues/18048
grep -v "unexpected ScalarExpr in uncorrelated plan" | # https://github.com/MaterializeInc/materialize/issues/18188
grep -v "internal error: cannot evaluate unmaterializable function" | # https://github.com/MaterializeInc/materialize/issues/14290
grep -v "Evaluation error: couldn't parse role id" | # https://github.com/MaterializeInc/materialize/issues/18705
grep -v "invalid digit found in string" | # https://github.com/MaterializeInc/materialize/issues/18706
grep -v "internal error: unexpected NULL" | # https://github.com/MaterializeInc/materialize/issues/20079
grep -v "couldn't parse role id" | # https://github.com/MaterializeInc/materialize/issues/18705
grep -v "is not of expected type ColumnType" | # https://github.com/MaterializeInc/materialize/issues/20077
grep -v "coalesce could not convert type time" | # https://github.com/MaterializeInc/materialize/issues/20082
sort | uniq -c | sort -n
