#include "config.h"

#include <iostream>
#include <chrono>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include <thread>
#include <typeinfo>
#include <sstream>

#include "random.hh"
#include "grammar.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "gitrev.h"

#include "log.hh"
#include "dump.hh"
#include "impedance.hh"
#include "dut.hh"

#ifdef HAVE_LIBSQLITE3
#include "sqlite.hh"
#endif

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "postgres.hh"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

using namespace std;

using namespace std::chrono;

extern "C" {
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

/* make the cerr logger globally accessible so we can emit one last
   report on SIGINT */
cerr_logger *global_cerr_logger;
json_logger *global_json_logger;

extern "C" void log_handler(int)
{
  if (global_cerr_logger)
    global_cerr_logger->report();
  if (global_json_logger)
    global_json_logger->report();
  exit(1);
}

int main(int argc, char *argv[])
{
  cerr << PACKAGE_NAME " " GITREV << endl;

  map<string,string> options;
  regex optregex("--(help|log-to|verbose|use-cluster|target|sqlite|monetdb|version|dump-all-graphs|dump-all-queries|seed|dry-run|max-queries|rng-state|exclude-catalog|explain-only|max-joins|log-json|dump-state|read-state)(?:=((?:.|\n)*))?");
  
  for(char **opt = argv+1 ;opt < argv+argc; opt++) {
    smatch match;
    string s(*opt);
    if (regex_match(s, match, optregex)) {
      options[string(match[1])] = match[2];
    } else {
      cerr << "Cannot parse option: " << *opt << endl;
      options["help"] = "";
    }
  }

  if (options.count("help")) {
    cerr <<
      "    --target=connstr     postgres database to send queries to" << endl <<
#ifdef HAVE_LIBSQLITE3
      "    --sqlite=URI         SQLite database to send queries to" << endl <<
#endif
#ifdef HAVE_MONETDB
      "    --monetdb=connstr    MonetDB database to send queries to" <<endl <<
#endif
      "    --log-to=connstr     log errors to postgres database" << endl <<
      "    --log-json           write out json result of run" << endl <<
      "    --seed=int           seed RNG with specified int instead of PID" << endl <<
      "    --dump-all-queries   print queries as they are generated" << endl <<
      "    --dump-all-graphs    dump generated ASTs" << endl <<
      "    --dump-state         dump the database state and exit" << endl <<
      "    --read-state         read the database state from stdin instead of from DB" << endl <<
      "    --dry-run            print queries instead of executing them" << endl <<
      "    --exclude-catalog    don't generate queries using catalog relations" << endl <<
      "    --explain-only       only run EXPLAIN queries" << endl <<
      "    --max-queries=long   terminate after generating this many queries" << endl <<
      "    --max-joins=long     max number of joins (1 by default)" << endl <<
      "    --rng-state=string   deserialize dumped rng state" << endl <<
      "    --use-cluster=string use a specified compute cluster" << endl <<
      "    --verbose            emit progress output" << endl <<
      "    --version            print version information and exit" << endl <<
      "    --help               print available command line options and exit" << endl;
    return 0;
  } else if (options.count("version")) {
    return 0;
  }

  try
    {
      shared_ptr<schema> schema;
      if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
	schema = make_shared<schema_sqlite>(options["sqlite"], options.count("exclude-catalog"));
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
	return 1;
#endif
      }
      else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB
	schema = make_shared<schema_monetdb>(options["monetdb"]);
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
	return 1;
#endif
      }
      else
      if (options.count("rng-state")) {
	   istringstream(options["rng-state"]) >> smith::rng;
      } else {
	   smith::rng.seed(options.count("seed") ? stoi(options["seed"]) : getpid());
      }

      vector<shared_ptr<logger> > loggers;

      loggers.push_back(make_shared<impedance_feedback>());

      if (options.count("verbose")) {
	auto l = make_shared<cerr_logger>();
	global_cerr_logger = &*l;
	loggers.push_back(l);
	signal(SIGINT, log_handler);
	signal(SIGTERM, log_handler);
      }

      if (options.count("log-json")) {
	auto l = make_shared<json_logger>();
	global_json_logger = &*l;
	loggers.push_back(l);
	signal(SIGINT, log_handler);
	signal(SIGTERM, log_handler);
      }

      if (options.count("dump-all-graphs"))
	loggers.push_back(make_shared<ast_logger>());

      if (options.count("dump-all-queries"))
	loggers.push_back(make_shared<query_dumper>());

      schema = make_shared<schema_pqxx>(options["target"], options.count("exclude-catalog"), options.count("dump-state"), options.count("read-state"));

      if (options.count("dump-state")) {
        return 0;
      }

      if (options.count("log-to"))
	loggers.push_back(make_shared<pqxx_logger>(
	     options.count("sqlite") ? options["sqlite"] : options["target"],
	     options["log-to"], *schema));

      scope scope;
      long queries_generated = 0;
      schema->fill_scope(scope);

      long max_joins = 1;
      if (options.count("max-joins"))
        max_joins = stol(options["max-joins"]);

      if (options.count("dry-run")) {
        assert(options.count("max-queries"));
        long max_queries = stol(options["max-queries"]);
        json data;
        data["version"] = "1.0";
        data["seed"] = options.count("seed") ? stoi(options["seed"]) : getpid();
        data["max-queries"] = max_queries;
        data["queries"] = json::array();
	while (1) {
          shared_ptr<prod> gen = options.count("explain-only") ? explain_factory(&scope, max_joins) : statement_factory(&scope, max_joins);
          stringstream s;
	  gen->out(s);
          data["queries"].push_back(s.str());
	  queries_generated++;

	  if (queries_generated >= max_queries) {
            std::cout << data.dump() << std::endl;
            return 0;
          }
	}
      }

      shared_ptr<dut_base> dut;
      
      if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
	dut = make_shared<dut_sqlite>(options["sqlite"]);
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
	return 1;
#endif
      }
      else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB	   
	dut = make_shared<dut_monetdb>(options["monetdb"]);
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
	return 1;
#endif
      }
      else
	dut = make_shared<dut_libpq>(options["target"], options.count("use-cluster") ? options["use-cluster"] : "");

      while (1) /* Loop to recover connection loss */
      {
	try {
            while (1) { /* Main loop */

	    if (options.count("max-queries")
		&& (++queries_generated > stol(options["max-queries"]))) {
	      if (global_cerr_logger)
		global_cerr_logger->report();
	      if (global_json_logger)
		global_json_logger->report();
	      return 0;
	    }
	    
	    /* Invoke top-level production to generate AST */
            shared_ptr<prod> gen = options.count("explain-only") ? explain_factory(&scope, max_joins) : statement_factory(&scope, max_joins);

	    for (auto l : loggers)
	      l->generated(*gen);
	  
	    /* Generate SQL from AST */
	    ostringstream s;
	    gen->out(s);

	    /* Try to execute it */
	    try {
	      dut->test(s.str());
	      for (auto l : loggers)
		l->executed(*gen);
	    } catch (const dut::failure &e) {
	      for (auto l : loggers)
		try {
		  l->error(*gen, e);
		} catch (runtime_error &e) {
		  cerr << endl << "log failed: " << typeid(*l).name() << ": "
		       << e.what() << endl;
		}
	      if ((dynamic_cast<const dut::broken *>(&e))) {
		/* re-throw to outer loop to recover session. */
		throw;
	      }
	    }
	  }
	}
	catch (const dut::broken &e) {
	  /* Give server some time to recover. */
	  this_thread::sleep_for(milliseconds(1000));
	}
      }
    }
  catch (const exception &e) {
    cerr << e.what() << endl;
    return 1;
  }
}
