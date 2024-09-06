#include "postgres.hh"
#include "random.hh"
#include "config.h"
#include <iostream>
#include <sstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

static regex e_timeout("ERROR:  canceling statement due to statement timeout(\n|.)*");
static regex e_syntax("ERROR:  syntax error at or near(\n|.)*");

bool pg_type::consistent(sqltype *rvalue)
{
  pg_type *t = dynamic_cast<pg_type*>(rvalue);

  if (!t) {
    cerr << "unknown type: " << rvalue->name  << endl;
    return false;
  }

  switch(typtype_) {
  case 'b': /* base type */
  case 'c': /* composite type */
  case 'd': /* domain */
  case 'r': /* range */
  case 'm': /* multirange */
  case 'e': /* enum */
  case 'l': /* MZ: list */
    return this == t;
    
  case 'p': /* pseudo type: accept any concrete matching type */
    if (name == "anyarray" || name == "anycompatiblearray") {
      return t->typelem_ != InvalidOid;
    } else if (name == "anynonarray" || name == "anycompatiblenonarray") {
      return t->typelem_ == InvalidOid;
    } else if (name == "anylist" || name == "anycompatiblelist") {
      return t->typelem_ != InvalidOid;
    } else if(name == "anyenum") {
      return t->typtype_ == 'e';
    } else if (name == "any" || name == "\"any\"" || name == "anycompatible") { /* as quoted by quote_ident() */
      return t->typtype_ != 'p'; /* any non-pseudo type */
    } else if (name == "anyelement") {
      return t->typelem_ == InvalidOid;
    } else if (name == "anyrange" || name == "anycompatiblerange") {
      return t->typtype_ == 'r';
    } else if (name == "anymultirange" || name == "anycompatiblemultirange") {
      return t->typtype_ == 'm';
    } else if (name == "record") {
      return t->typtype_ == 'c';
    } else if (name == "cstring") {
      return this == t;
    } else if (name == "map" || name == "anycompatiblemap") {
      return t->name.rfind("map", 0) == 0;
    } else {
      return false;
    }
      
  default:
    throw std::logic_error("unknown typtype");
  }
}

dut_pqxx::dut_pqxx(std::string conninfo)
  : c(conninfo)
{
     c.set_variable("statement_timeout", "'10s'");
     c.set_variable("client_min_messages", "'ERROR'");
     c.set_variable("application_name", "'" PACKAGE "::dut'");
}

void dut_pqxx::test(const std::string &stmt)
{
  try {
    pqxx::work w(c);
    w.exec("SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'");
    //w.exec("SET CLUSTER_REPLICA = default.r1");
    w.exec(stmt.c_str());
    if (d6() < 4)
      w.abort();
    else
      w.commit();
  } catch (const pqxx::failure &e) {
    if ((dynamic_cast<const pqxx::broken_connection *>(&e))) {
      /* re-throw to outer loop to recover session. */
      throw dut::broken(e.what());
    }

    if (regex_match(e.what(), e_timeout))
      throw dut::timeout(e.what());
    else if (regex_match(e.what(), e_syntax))
      throw dut::syntax(e.what());
    else
      throw dut::failure(e.what());
  }
}


schema_pqxx::schema_pqxx(std::string &conninfo, bool no_catalog, bool dump_state, bool read_state) : c(conninfo)
{
  json data;

  if (read_state) {
    data = json::parse(std::cin);
  }

  c.set_variable("application_name", "'" PACKAGE "::schema'");

  pqxx::work w(c);
  w.exec("SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'");
  pqxx::result r;

  string procedure_is_aggregate = "mz_functions.name in ('array_agg', 'avg', 'avg_internal_v1', 'bit_and', 'bit_or', 'bit_xor', 'bool_and', 'bool_or', 'count', 'every', 'json_agg', 'jsonb_agg', 'json_object_agg', 'jsonb_object_agg', 'list_agg', 'max', 'min', 'range_agg', 'range_intersect_agg', 'string_agg', 'sum', 'xmlagg', 'corr', 'covar_pop', 'covar_samp', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'stddev', 'stddev_pop', 'stddev_samp', 'variance', 'var_pop', 'var_samp', 'mode', 'percentile_cont', 'percentile_disc', 'rank', 'dense_rank', 'percent_rank', 'grouping', 'mz_all', 'mz_any')";
  string procedure_is_window = "mz_functions.name in ('row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist', 'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value')";

  cerr << "Loading types...";

  if (read_state) {
    for (const auto &obj : data["types"]) {
      pg_type *t = new pg_type(obj["name"].get<string>(),obj["oid"].get<OID>(),obj["typdelim"].get<string>()[0],obj["typrelid"].get<OID>(), obj["typelem"].get<OID>(), obj["typarray"].get<OID>(), obj["typtype"].get<string>()[0]);
      oid2type[obj["oid"]] = t;
      name2type[obj["name"]] = t;
      types.push_back(t);
    }
  } else {
    if (dump_state) {
      data["types"] = json::array();
    }
    r = w.exec("select typname, "
               "oid, ',' as typdelim, typrelid, typelem, typarray, typtype "
               "from pg_type ");
    for (auto row = r.begin(); row != r.end(); ++row) {
      string name(row[0].as<string>());
      OID oid(row[1].as<OID>());
      string typdelim(row[2].as<string>());
      OID typrelid(row[3].as<OID>());
      OID typelem(row[4].as<OID>());
      OID typarray(row[5].as<OID>());
      string typtype(row[6].as<string>());
      //       if (schema == "pg_catalog")
      // 	continue;
      //       if (schema == "mz_catalog")
      // 	continue;
      //       if (schema == "mz_internal")
      // 	continue;
      //       if (schema == "information_schema")
      // 	continue;
      // Not sure if this is better or worse:
      //if (name == "map")
      //  name = "map[text=>text]";

      pg_type *t = new pg_type(name,oid,typdelim[0],typrelid, typelem, typarray, typtype[0]);
      oid2type[oid] = t;
      name2type[name] = t;
      types.push_back(t);

      if (dump_state) {
        json obj;
        obj["name"] = name;
        obj["oid"] = oid;
        obj["typdelim"] = typdelim;
        obj["typrelid"] = typrelid;
        obj["typelem"] = typelem;
        obj["typarray"] = typarray;
        obj["typtype"] = typtype;
        data["types"].push_back(obj);
      }
    }
  }

  booltype = name2type["bool"];
  inttype = name2type["int4"];

  //internaltype = name2type["internal"];
  arraytype = name2type["anyarray"];

  cerr << "done." << endl;

  cerr << "Loading tables...";
  // Reread table data since it might have changed
  if (read_state && !dump_state) {
    for (const auto &obj : data["tables"]) {
      string schema = obj["schema"].get<string>();
      string db = obj["db"].get<string>();
      if (no_catalog && ((schema == "pg_catalog") || (schema == "mz_catalog") || (schema == "mz_internal") || (schema == "information_schema")))
        continue;

      tables.push_back(table(w.quote_name(obj["name"].get<string>()),
                             w.quote_name(schema),
                             db.empty() ? db : w.quote_name(db),
                             ((obj["table_type"].get<string>() == "BASE TABLE") ? true : false)));
    }
  } else {
    r = w.exec("SELECT "
         "    r.name AS table_name, "
         "    s.name AS table_schema, "
         "    COALESCE(d.name, '') as table_catalog, "
         "    CASE r.type "
         "        WHEN 'materialized-view' THEN 'MATERIALIZED VIEW' "
         "        WHEN 'table' THEN 'BASE TABLE' "
         "        ELSE pg_catalog.upper(r.type) "
         "    END AS table_type "
         "FROM mz_catalog.mz_relations r "
         "JOIN mz_catalog.mz_schemas s ON s.id = r.schema_id "
         "LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id "
         "where r.name not like 'mz_dataflow_operator_reachability%' " // https://github.com/MaterializeInc/materialize/issues/18296
         "and r.name not like '%_raw' " // Can be huge, easy to go OoM
         );
    if (dump_state) {
      data["tables"] = json::array();
    }
    for (auto row = r.begin(); row != r.end(); ++row) {
      string name(row[0].as<string>());
      string schema(row[1].as<string>());
      string db(row[2].as<string>());
      string table_type(row[3].as<string>());

      if (no_catalog && ((schema == "pg_catalog") || (schema == "mz_catalog") || (schema == "mz_internal") || (schema == "information_schema")))
        continue;

      tables.push_back(table(w.quote_name(name),
                             w.quote_name(schema),
                             db.empty() ? db : w.quote_name(db),
                             ((table_type == "BASE TABLE") ? true : false)));

      if (dump_state) {
        json obj;
        obj["name"] = name,
        obj["schema"] = schema;
        obj["db"] = db;
        obj["table_type"] = table_type;
        data["tables"].push_back(obj);
      }
    }
  }
  assert(tables.size() > 0);

  cerr << "done." << endl;

  cerr << "Loading columns and constraints...";

  int table_index = 0;
  for (auto t = tables.begin(); t != tables.end(); ++t) {
    // Reread table data since it might have changed
    if (read_state && !dump_state) {
      for (const auto &obj : data["tables"][table_index]["columns"]) {
        column c(obj["name"].get<string>(), oid2type[obj["type"].get<OID>()]);
        t->columns().push_back(c);
      }
      for (const auto &obj : data["tables"][table_index]["constraints"]) {
        t->constraints.push_back(obj.get<string>());
      }
    } else {
      if (dump_state) {
        data["tables"][table_index]["columns"] = json::array();
        data["tables"][table_index]["constraints"] = json::array();
      }
      string q("select attname, "
               "atttypid "
               "from pg_attribute join pg_class c on( c.oid = attrelid ) "
               "join pg_namespace n on n.oid = relnamespace "
               "where not attisdropped "
               "and not (nspname in ('mz_catalog', 'pg_catalog', 'mz_internal', 'information_schema') and atttypid = 18) " // Expected, see https://github.com/MaterializeInc/materialize/issues/17899
               "and attname not in "
               "('xmin', 'xmax', 'ctid', 'cmin', 'cmax', 'tableoid', 'oid') ");
      q += " and relname = " + w.quote(t->name);
      q += " and nspname = " + w.quote(t->schema);

      r = w.exec(q);
      for (auto row : r) {
        column c(w.quote_name(row[0].as<string>()), oid2type[row[1].as<OID>()]);
        t->columns().push_back(c);

        if (dump_state) {
          json obj;
          obj["name"] = w.quote_name(row[0].as<string>());
          obj["type"] = row[1].as<OID>();
          data["tables"][table_index]["columns"].push_back(obj);
        }
      }

      q = "select conname from pg_class t "
        "join pg_constraint c on (t.oid = c.conrelid) "
        "where contype in ('f', 'u', 'p') ";
      q += " and relnamespace = (select oid from pg_namespace where nspname = " + w.quote(t->schema) + ")";
      q += " and relname = " + w.quote(t->name);

      for (auto row : w.exec(q)) {
        t->constraints.push_back(row[0].as<string>());

        if (dump_state) {
          data["tables"][table_index]["constraints"].push_back(row[0].as<string>());
        }
      }
    }
    table_index++;
  }
  cerr << "done." << endl;

  cerr << "Loading operators...";

  if (read_state) {
    for (const auto &obj : data["operators"]) {
      op o(obj["name"].get<string>(),
           oid2type[obj["left_type"].get<OID>()],
           oid2type[obj["right_type"].get<OID>()],
           oid2type[obj["return_type"].get<OID>()]);
      register_operator(o);
    }
  } else {
    r = w.exec("SELECT "
      "mz_operators.name AS oprname, "
      "left_type.oid as oprleft, "
      "right_type.oid as oprright, "
      "ret_type.oid AS oprresult "
      "FROM mz_catalog.mz_operators "
      "JOIN mz_catalog.mz_types AS ret_type "
      "ON mz_operators.return_type_id = ret_type.id "
      "JOIN mz_catalog.mz_types AS left_type "
      "ON mz_operators.argument_type_ids[1] = left_type.id "
      "JOIN mz_catalog.mz_types AS right_type "
      "ON mz_operators.argument_type_ids[2] = right_type.id "
      "WHERE array_length(mz_operators.argument_type_ids, 1) = 2 "
      "AND mz_operators.name <> '<@' " // type system insufficient
      "AND mz_operators.name <> '@>' " // type system insufficient
      "UNION SELECT "
      "mz_operators.name AS oprname, "
      "0 as oprleft, "
      "right_type.oid as oprright, "
      "ret_type.oid AS oprresult "
      "FROM mz_catalog.mz_operators "
      "JOIN mz_catalog.mz_types AS ret_type ON mz_operators.return_type_id = ret_type.id "
      "JOIN mz_catalog.mz_types AS right_type ON mz_operators.argument_type_ids[1] = right_type.id "
      "WHERE array_length(mz_operators.argument_type_ids, 1) = 1");

    if (dump_state) {
      data["operators"] = json::array();
    }
    for (auto row : r) {
      op o(row[0].as<string>(),
           oid2type[row[1].as<OID>()],
           oid2type[row[2].as<OID>()],
           oid2type[row[3].as<OID>()]);
      register_operator(o);

      if (dump_state) {
        json obj;
        obj["name"] = row[0].as<string>();
        obj["left_type"] = row[1].as<OID>();
        obj["right_type"] = row[2].as<OID>();
        obj["return_type"] = row[3].as<OID>();
        data["operators"].push_back(obj);
      }
    }
  }

  cerr << "done." << endl;

  cerr << "Loading routines...";
  if (read_state) {
    for (const auto &obj : data["routines"]) {
      routine proc(obj["schema_name"].get<string>(),
                   obj["oid"].get<string>(),
                   oid2type[obj["return_type"].get<OID>()],
                   obj["name"].get<string>(),
                   obj["returns_set"].get<bool>());
      register_routine(proc);
    }
  } else {
    r = w.exec(
      "SELECT "
      "  mz_schemas.name AS nspname, "
      "  mz_functions.oid, "
      "  ret_type.oid AS prorettype, "
      "  mz_functions.name AS proname, "
      "  mz_functions.returns_set "
      "FROM mz_catalog.mz_functions "
      "JOIN mz_catalog.mz_schemas "
      "ON mz_functions.schema_id = mz_schemas.id "
      "JOIN mz_catalog.mz_types AS ret_type "
      "ON mz_functions.return_type_id = ret_type.id "
      "WHERE mz_functions.name <> 'pg_event_trigger_table_rewrite_reason' "
      "AND mz_functions.name <> 'pg_event_trigger_table_rewrite_oid' "
      "AND mz_functions.name !~ '^ri_fkey_' "
      "AND mz_functions.name <> 'mz_panic' " // don't want crashes
      "AND mz_functions.name <> 'mz_error_if_null' " // don't want errors with random messages
      "AND mz_functions.name <> 'mz_logical_timestamp' " // mz_logical_timestamp() has been renamed to mz_now()
      "AND mz_functions.name <> 'mz_sleep' " // https://github.com/MaterializeInc/materialize/issues/17984
      "AND mz_functions.name <> 'date_bin' " // binary date_bin is unsupported
      "AND mz_functions.name <> 'list_length_max' " // list_length_max is unsupported
      "AND mz_functions.name <> 'list_n_layers' " // list_n_layers is unsupported
      "AND mz_functions.name <> 'list_remove' " // list_remove is unsupported
      "AND mz_functions.name <> 'concat_agg' " // concat_agg not yet supported
      "AND NOT mz_functions.name like '%\\_in' " // all _in functions are not yet supported
      "AND NOT mz_functions.name in ('aclitemin', 'boolin', 'bpcharin', 'byteain', 'charin', 'float4in', 'float8in', 'int2in', 'int2vectorin', 'int4in', 'int8in', 'namein', 'oidin', 'regclassin', 'regprocin', 'regtypein', 'textin', 'varcharin') " // all in functions are not yet supported, but don't exclude functions like 'min', 'sin', date_bin', so better keep an explicit list
      "AND mz_functions.name <> 'mz_row_size' " // mz_row_size requires a record type
      "AND mz_functions.name <> 'jsonb_build_object' " // argument list must have even number of elements
      "AND mz_functions.name <> 'mz_now' " // https://github.com/MaterializeInc/materialize/issues/18045
      "AND NOT mz_functions.name like 'has_%_privilege' " // common "does not exist" errors
      "AND NOT mz_functions.name like 'mz_%_oid' " // common "does not exist" errors
      "AND mz_functions.name <> 'mz_global_id_to_name' " // common "does not exist" errors
      "AND mz_functions.name <> 'date_bin_hopping' " // the date_bin_hopping function is not supported
      "AND mz_functions.name <> 'generate_series' " // out of memory on large data sets
      "AND NOT mz_functions.name like '%recv' " // https://github.com/MaterializeInc/materialize/issues/17870
      "AND mz_functions.name <> 'pg_cancel_backend' " // pg_cancel_backend in this position not yet supported
      "AND (mz_functions.name <> 'sum' OR mz_functions.return_type_id <> (select id from mz_types where name = 'interval'))" // sum(interval) not yet supported, see https://github.com/MaterializeInc/materialize/issues/18043
      "AND (mz_functions.name <> 'timezone' OR mz_functions.argument_type_ids[2] <> (select id from mz_types where name = 'time'))" // timezone with time type is intentionally not supported, see https://github.com/MaterializeInc/materialize/pull/22960
      "AND mz_functions.name <> 'pretty_sql' " // Expected a keyword at the beginning of a statement, found ...
      "AND NOT (" + procedure_is_aggregate + " or " + procedure_is_window + ") ");

    if (dump_state) {
      data["routines"] = json::array();
    }
    for (auto row : r) {
      routine proc(row[0].as<string>(),
                   row[1].as<string>(),
                   oid2type[row[2].as<long>()],
                   row[3].as<string>(),
                   row[4].as<bool>());
      register_routine(proc);

      if (dump_state) {
        json obj;
        obj["schema_name"] = row[0].as<string>();
        obj["oid"] = row[1].as<string>();
        obj["return_type"] = row[2].as<long>();
        obj["name"] = row[3].as<string>();
        obj["returns_set"] = row[4].as<bool>();
        data["routines"].push_back(obj);
      }
    }
  }

  cerr << "done." << endl;

  cerr << "Loading routine parameters...";

  int routine_index = 0;
  for (auto &proc : routines) {
    if (read_state) {
      for (const auto &obj : data["routines"][routine_index]["parameters"]) {
          proc.argtypes.push_back(oid2type[obj.get<OID>()]);
      }
    } else {
      // unnest is broken: https://github.com/MaterializeInc/materialize/issues/17979
      //string q("select (select oid from mz_types where a = id) from mz_functions, lateral unnest(argument_type_ids) as a where oid = ");
      string q("select array_to_string(argument_type_ids, ',') from mz_functions where oid = ");
      q += w.quote(proc.specific_name);

      r = w.exec(q);
      string previous = "NONE";
      OID oid = 0;
      if (dump_state) {
        data["routines"][routine_index]["parameters"] = json::array();
      }
      for (auto row : r) {
        string s = row[0].as<string>();
        string segment = "";
        std::stringstream test(s);
        while (std::getline(test, segment, ',')) {
          if (segment != previous) {
            string q("select oid from mz_types where id = ");
            q += w.quote(segment);
            pqxx::result r2 = w.exec(q);
            previous = segment;
            oid = r2[0][0].as<OID>();
          }
          sqltype *t = oid2type[oid];
          assert(t);
          proc.argtypes.push_back(t);
          if (dump_state) {
            data["routines"][routine_index]["parameters"].push_back(oid);
          }
        }
      }
    }
    routine_index++;
  }
  cerr << "done." << endl;

  cerr << "Loading aggregates...";
  if (read_state) {
    for (const auto &obj : data["aggregates"]) {
      routine proc(obj["schema_name"].get<string>(),
                   obj["oid"].get<string>(),
                   oid2type[obj["return_type"].get<OID>()],
                   obj["name"].get<string>(),
                   obj["returns_set"].get<bool>());
      register_aggregate(proc);
    }
  } else {
    if (dump_state) {
      data["aggregates"] = json::array();
    }
    // Loading aggregates...ERROR:  WHERE clause error: column "prorettype" does not exist
    r = w.exec("SELECT "
      "  mz_schemas.name AS nspname, "
      "  mz_functions.oid, "
      "  ret_type.oid AS prorettype, "
      "  mz_functions.name AS proname, "
      "  mz_functions.returns_set "
      "FROM mz_catalog.mz_functions "
      "JOIN mz_catalog.mz_schemas "
      "ON mz_functions.schema_id = mz_schemas.id "
      "JOIN mz_catalog.mz_types AS ret_type "
      "ON mz_functions.return_type_id = ret_type.id "
      "WHERE mz_functions.name not in ('pg_event_trigger_table_rewrite_reason', 'percentile_cont', 'dense_rank', 'cume_dist', 'rank', 'test_rank', 'percent_rank', 'percentile_disc', 'mode', 'test_percentile_disc') "
      "AND mz_functions.name !~ '^ri_fkey_' "
      "AND NOT (mz_functions.name in ('sum', 'avg', 'avg_internal_v1') AND ret_type.oid = 1186) " // https://github.com/MaterializeInc/materialize/issues/18043
      "AND mz_functions.name <> 'array_agg' " // https://github.com/MaterializeInc/materialize/issues/18044
      "AND NOT (mz_functions.name = 'string_agg' AND ret_type.oid = 17) " // string_agg on BYTEA not yet supported
      "AND NOT mz_functions.name in ('mz_all', 'mz_any') " // https://github.com/MaterializeInc/materialize/issues/18057
      "AND " + procedure_is_aggregate + " AND NOT " + procedure_is_window);
    for (auto row : r) {
      routine proc(row[0].as<string>(),
                   row[1].as<string>(),
                   oid2type[row[2].as<long>()],
                   row[3].as<string>(),
                   row[4].as<bool>());
      register_aggregate(proc);

      if (dump_state) {
        json obj;
        obj["schema_name"] = row[0].as<string>();
        obj["oid"] = row[1].as<string>();
        obj["return_type"] = row[2].as<long>();
        obj["name"] = row[3].as<string>();
        obj["returns_set"] = row[4].as<bool>();
        data["aggregates"].push_back(obj);
      }
    }
  }

  cerr << "done." << endl;

  cerr << "Loading aggregate parameters...";

  int aggregate_index = 0;
  for (auto &proc : aggregates) {
    // unnest is broken: https://github.com/MaterializeInc/materialize/issues/17979
    //string q("select (select oid from mz_types where a = id) from mz_functions, lateral unnest(argument_type_ids) as a where oid = ");
    if (read_state) {
      for (const auto &obj : data["aggregates"][aggregate_index]["parameters"]) {
          proc.argtypes.push_back(oid2type[obj.get<OID>()]);
      }
    } else {
      string q("select array_to_string(argument_type_ids, ',') from mz_functions where oid = ");
      q += w.quote(proc.specific_name);

      r = w.exec(q);
      string previous = "NONE";
      OID oid = 0;
      if (dump_state) {
        data["aggregates"][aggregate_index]["parameters"] = json::array();
      }
      for (auto row : r) {
        string s = row[0].as<string>();
        string segment = "";
        std::stringstream test(s);
        while (std::getline(test, segment, ',')) {
          if (segment != previous) {
            string q("select oid from mz_types where id = ");
            q += w.quote(segment);
            pqxx::result r2 = w.exec(q);
            previous = segment;
            oid = r2[0][0].as<OID>();
          }
          sqltype *t = oid2type[oid];
          assert(t);
          proc.argtypes.push_back(t);
          if (dump_state) {
            data["aggregates"][aggregate_index]["parameters"].push_back(oid);
          }
        }
      }
    }
    aggregate_index++;
  }
  cerr << "done." << endl;
#ifdef HAVE_LIBPQXX7
  c.close();
#else
  c.disconnect();
#endif

  if (dump_state) {
    std::cout << data.dump(4) << std::endl;
  }

  generate_indexes();
}

extern "C" {
    void dut_libpq_notice_rx(void *arg, const PGresult *res);
}

void dut_libpq_notice_rx(void *arg, const PGresult *res)
{
    (void) arg;
    (void) res;
}

void dut_libpq::connect(std::string &conninfo)
{
    if (conn) {
	PQfinish(conn);
    }
    conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(conn) != CONNECTION_OK)
    {
	char *errmsg = PQerrorMessage(conn);
	if (strlen(errmsg))
	    throw dut::broken(errmsg, "08001");
    }

    command("set statement_timeout to '10s'");
    command("set client_min_messages to 'ERROR';");
    command("set application_name to '" PACKAGE "::dut';");

    if (!use_cluster_.empty()) {
      command("set cluster to '" + use_cluster_ + "'");
    }

    PQsetNoticeReceiver(conn, dut_libpq_notice_rx, (void *) 0);
}

dut_libpq::dut_libpq(std::string conninfo, const string &use_cluster)
    : conninfo_(conninfo)
    , use_cluster_ (use_cluster)
{
    connect(conninfo);
}

void dut_libpq::command(const std::string &stmt)
{
    if (!conn)
	connect(conninfo_);
    PGresult *res = PQexec(conn, stmt.c_str());

    switch (PQresultStatus(res)) {

    case PGRES_FATAL_ERROR:
    default:
    {
	const char *errmsg = PQresultErrorMessage(res);
	if (!errmsg || !strlen(errmsg))
	     errmsg = PQerrorMessage(conn);

	const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
	if (!sqlstate || !strlen(sqlstate))
	     sqlstate =  (CONNECTION_OK != PQstatus(conn)) ? "08000" : "?????";
	
	std::string error_string(errmsg);
	std::string sqlstate_string(sqlstate);
	PQclear(res);

	if (CONNECTION_OK != PQstatus(conn)) {
            PQfinish(conn);
	    conn = 0;
	    throw dut::broken(error_string.c_str(), sqlstate_string.c_str());
	}
	if (sqlstate_string == "42601")
	     throw dut::syntax(error_string.c_str(), sqlstate_string.c_str());
	else
	     throw dut::failure(error_string.c_str(), sqlstate_string.c_str());
    }

    case PGRES_NONFATAL_ERROR:
    case PGRES_TUPLES_OK:
    case PGRES_SINGLE_TUPLE:
    case PGRES_COMMAND_OK:
	PQclear(res);
	return;
    }
}

void dut_libpq::test(const std::string &stmt)
{
    command(stmt.c_str());
}
