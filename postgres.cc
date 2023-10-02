#include "postgres.hh"
#include "random.hh"
#include "config.h"
#include <iostream>
#include <sstream>

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


schema_pqxx::schema_pqxx(std::string &conninfo, bool no_catalog) : c(conninfo)
{
  c.set_variable("application_name", "'" PACKAGE "::schema'");

  pqxx::work w(c);
  w.exec("SET TRANSACTION_ISOLATION TO 'SERIALIZABLE'");

  string procedure_is_aggregate = "mz_functions.name in ('array_agg', 'avg', 'avg_internal_v1', 'bit_and', 'bit_or', 'bit_xor', 'bool_and', 'bool_or', 'count', 'every', 'json_agg', 'jsonb_agg', 'json_object_agg', 'jsonb_object_agg', 'list_agg', 'max', 'min', 'range_agg', 'range_intersect_agg', 'string_agg', 'sum', 'xmlagg', 'corr', 'covar_pop', 'covar_samp', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'stddev', 'stddev_pop', 'stddev_samp', 'variance', 'var_pop', 'var_samp', 'mode', 'percentile_cont', 'percentile_disc', 'rank', 'dense_rank', 'percent_rank', 'grouping', 'mz_all', 'mz_any')";
  string procedure_is_window = "mz_functions.name in ('row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist', 'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value')";

  cerr << "Loading types...";

  pqxx::result r = w.exec("select typname, "
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
  }

  booltype = name2type["bool"];
  inttype = name2type["int4"];

  //internaltype = name2type["internal"];
  arraytype = name2type["anyarray"];

  cerr << "done." << endl;

  cerr << "Loading tables...";
  r = w.exec("select table_name, "
		    "table_schema, "
	            "true, " //"is_insertable_into, " # column "is_insertable_into" does not exist
	            "table_type "
	     "from information_schema.tables "
       "where table_name not like 'mz_dataflow_operator_reachability%' " // https://github.com/MaterializeInc/materialize/issues/18296
       );
  for (auto row = r.begin(); row != r.end(); ++row) {
    string schema(row[1].as<string>());
    string insertable(row[2].as<string>());
    string table_type(row[3].as<string>());

	if (no_catalog && ((schema == "pg_catalog") || (schema == "mz_catalog") || (schema == "mz_internal") || (schema == "information_schema")))
		continue;
      
    tables.push_back(table(row[0].as<string>(),
			   schema,
			   ((insertable == "YES") ? true : false),
			   ((table_type == "BASE TABLE") ? true : false)));
  }
  assert(tables.size() > 0);

  r = w.exec("select matviewname, "
            "schemaname, "
                "'NO', "
                "'VIEW' "
         "from pg_matviews");

  for (auto row = r.begin(); row != r.end(); ++row) {
    string schema(row[1].as<string>());
    string insertable(row[2].as<string>());
    string table_type(row[3].as<string>());

    if (no_catalog && ((schema == "pg_catalog") || (schema == "mz_catalog") || (schema == "mz_internal") || (schema == "information_schema")))
        continue;

    tables.push_back(table(row[0].as<string>(),
               schema,
               ((insertable == "YES") ? true : false),
               ((table_type == "BASE TABLE") ? true : false)));
  }

  cerr << "done." << endl;

  cerr << "Loading columns and constraints...";

  for (auto t = tables.begin(); t != tables.end(); ++t) {
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
      column c(row[0].as<string>(), oid2type[row[1].as<OID>()]);
      t->columns().push_back(c);
    }

    q = "select conname from pg_class t "
      "join pg_constraint c on (t.oid = c.conrelid) "
      "where contype in ('f', 'u', 'p') ";
    q += " and relnamespace = " " (select oid from pg_namespace where nspname = " + w.quote(t->schema) + ")";
    q += " and relname = " + w.quote(t->name);

    for (auto row : w.exec(q)) {
      t->constraints.push_back(row[0].as<string>());
    }
    
  }
  cerr << "done." << endl;

  cerr << "Loading operators...";

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
    "UNION SELECT "
    "mz_operators.name AS oprname, "
    "0 as oprleft, "
    "right_type.oid as oprright, "
    "ret_type.oid AS oprresult "
    "FROM mz_catalog.mz_operators "
    "JOIN mz_catalog.mz_types AS ret_type ON mz_operators.return_type_id = ret_type.id "
    "JOIN mz_catalog.mz_types AS right_type ON mz_operators.argument_type_ids[1] = right_type.id "
    "WHERE array_length(mz_operators.argument_type_ids, 1) = 1");

  for (auto row : r) {
    op o(row[0].as<string>(),
	 oid2type[row[1].as<OID>()],
	 oid2type[row[2].as<OID>()],
	 oid2type[row[3].as<OID>()]);
    register_operator(o);
  }

  cerr << "done." << endl;

  cerr << "Loading routines...";
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
    "AND mz_functions.name <> 'array_in' " // array_in not yet supported
    "AND mz_functions.name <> 'mz_row_size' " // mz_row_size requires a record type
    "AND mz_functions.name <> 'jsonb_build_object' " // argument list must have even number of elements
    "AND mz_functions.name <> 'mz_now' " // https://github.com/MaterializeInc/materialize/issues/18045
    "AND NOT mz_functions.name like 'has_%_privilege' " // common "does not exist" errors
    "AND NOT mz_functions.name like 'mz_%_oid' " // common "does not exist" errors
    "AND mz_functions.name <> 'mz_global_id_to_name' " // common "does not exist" errors
    "AND mz_functions.name <> 'date_bin_hopping' " // the date_bin_hopping function is not supported
    "AND mz_functions.name <> 'generate_series' " // out of memory on large data sets
    "AND NOT (" + procedure_is_aggregate + " or " + procedure_is_window + ") ");

  for (auto row : r) {
    routine proc(row[0].as<string>(),
		 row[1].as<string>(),
		 oid2type[row[2].as<long>()],
		 row[3].as<string>(),
		 row[4].as<bool>());
    register_routine(proc);
  }

  cerr << "done." << endl;

  cerr << "Loading routine parameters...";

  for (auto &proc : routines) {
    // unnest is broken: https://github.com/MaterializeInc/materialize/issues/17979
    //string q("select (select oid from mz_types where a = id) from mz_functions, lateral unnest(argument_type_ids) as a where oid = ");
    string q("select array_to_string(argument_type_ids, ',') from mz_functions where oid = ");
    q += w.quote(proc.specific_name);

    r = w.exec(q);
    string previous = "NONE";
    OID oid = 0;
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
      }
    }
  }
  cerr << "done." << endl;

  cerr << "Loading aggregates...";
  // Loading aggregates...ERROR:  WHERE clause error: column "prorettype" does not exist
  r = w.exec("SELECT "
    "  mz_schemas.name AS nspname, "
    "  mz_functions.oid, "
    "  ret_type.oid AS prorettype, "
    "  mz_functions.name AS proname "
    "FROM mz_catalog.mz_functions "
    "JOIN mz_catalog.mz_schemas "
    "ON mz_functions.schema_id = mz_schemas.id "
    "JOIN mz_catalog.mz_types AS ret_type "
    "ON mz_functions.return_type_id = ret_type.id "
    "WHERE mz_functions.name not in ('pg_event_trigger_table_rewrite_reason', 'percentile_cont', 'dense_rank', 'cume_dist', 'rank', 'test_rank', 'percent_rank', 'percentile_disc', 'mode', 'test_percentile_disc') "
    "AND mz_functions.name !~ '^ri_fkey_' "
    "AND NOT (mz_functions.name in ('sum', 'avg') AND ret_type.oid = 1186) " // https://github.com/MaterializeInc/materialize/issues/18043
    "AND mz_functions.name <> 'array_agg' " // https://github.com/MaterializeInc/materialize/issues/18044
    "AND NOT mz_functions.name in ('mz_all', 'mz_any') " // https://github.com/MaterializeInc/materialize/issues/18057
    "AND " + procedure_is_aggregate + " AND NOT " + procedure_is_window);
  for (auto row : r) {
    routine proc(row[0].as<string>(),
		 row[1].as<string>(),
		 oid2type[row[2].as<OID>()],
		 row[3].as<string>());
    register_aggregate(proc);
  }

  cerr << "done." << endl;

  cerr << "Loading aggregate parameters...";

  for (auto &proc : aggregates) {
    // unnest is broken: https://github.com/MaterializeInc/materialize/issues/17979
    //string q("select (select oid from mz_types where a = id) from mz_functions, lateral unnest(argument_type_ids) as a where oid = ");
    string q("select array_to_string(argument_type_ids, ',') from mz_functions where oid = ");
    q += w.quote(proc.specific_name);

    r = w.exec(q);
    string previous = "NONE";
    OID oid = 0;
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
      }
    }
  }
  cerr << "done." << endl;
  c.disconnect();
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
