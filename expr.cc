#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <memory>
#include <cassert>

#include "random.hh"
#include "relmodel.hh"
#include "grammar.hh"
#include "schema.hh"
#include "impedance.hh"
#include "expr.hh"

using namespace std;
using impedance::matched;

shared_ptr<value_expr> value_expr::factory(prod *p, sqltype *type_constraint, bool can_return_set)
{
  try {
    if (1 == d20() && p->level < d6() && window_function::allowed(p))
      return make_shared<window_function>(p, type_constraint);
    if (1 == d42() && p->level < d6() && type_constraint && type_constraint->name.rfind("list", 0) != 0 && type_constraint->name.rfind("map", 0) != 0 && type_constraint->name.rfind("record", 0) != 0 && type_constraint->name.rfind("any", 0) != 0)
      return make_shared<coalesce>(p, type_constraint);
    else if (1 == d42() && p->level < d6() && type_constraint && type_constraint->name.rfind("list", 0) != 0 && type_constraint->name.rfind("map", 0) != 0 && type_constraint->name.rfind("record", 0) != 0 && type_constraint->name.rfind("any", 0) != 0)
      return make_shared<nullif>(p, type_constraint);
    else if (p->level < d6() && d6() < 3)
      return make_shared<funcall>(p, type_constraint, can_return_set);
    else if (p->level < d6() && d6() == 6)
      return make_shared<opcall>(p, type_constraint);
    else if (d12()==1)
      return make_shared<atomic_subselect>(p, type_constraint);
    else if (p->level< d6() && d9()==1)
      return make_shared<case_expr>(p, type_constraint);
    else if (p->scope->refs.size() && d20() > 1)
      return make_shared<column_reference>(p, type_constraint);
    else
      return make_shared<const_expr>(p, type_constraint);
  } catch (runtime_error &e) {
  }
  p->retry();
  return factory(p, type_constraint, can_return_set);
}

case_expr::case_expr(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  condition = bool_expr::factory(this);
  true_expr = value_expr::factory(this, type_constraint, false);
  false_expr = value_expr::factory(this, true_expr->type, false);

  retry_limit = 20;
  while(false_expr->type != true_expr->type) {
       retry();
       /* Types are consistent but not identical.  Try to find a more
	  concrete one for a better match. */
       if (true_expr->type->consistent(false_expr->type))
	    true_expr = value_expr::factory(this, false_expr->type, false);
       else
	    false_expr = value_expr::factory(this, true_expr->type, false);
  }
  type = true_expr->type;
}

void case_expr::out(std::ostream &out)
{
  out << "case when " << *condition;
  out << " then " << *true_expr;
  out << " else " << *false_expr;
  out << " end";
  indent(out);
}

void case_expr::accept(prod_visitor *v)
{
  v->visit(this);
  condition->accept(v);
  true_expr->accept(v);
  false_expr->accept(v);
}

column_reference::column_reference(prod *p, sqltype *type_constraint) : value_expr(p)
{
  if (type_constraint) {
    auto pairs = scope->refs_of_type(type_constraint);
    auto picked = random_pick(pairs);
    reference += picked.first->ident()
      + "." + scope->schema->quote_name(picked.second.name);
    type = picked.second.type;
    assert(type_constraint->consistent(type));
  } else {
    named_relation *r = random_pick(scope->refs);

    reference += r->ident() + ".";
    column &c = random_pick(r->columns());
    type = c.type;
    reference += scope->schema->quote_name(c.name);
  }
}

shared_ptr<bool_expr> bool_expr::factory(prod *p)
{
  try {
       if (p->level > d100())
	    return make_shared<truth_value>(p);
       if(d6() < 4)
	    return make_shared<comparison_op>(p);
       else if (d6() < 4)
	    return make_shared<bool_term>(p);
       else if (d6() < 4)
	    return make_shared<null_predicate>(p);
       else if (d6() < 4 && g_joins > 0) {
	    g_joins--;
	    return make_shared<exists_predicate>(p);
       }
       else if (d6() < 3)
	    return make_shared<distinct_pred>(p);
       else
	    return make_shared<truth_value>(p);
  } catch (runtime_error &e) {
  }
  p->retry();
  return factory(p);
     
}

exists_predicate::exists_predicate(prod *p) : bool_expr(p)
{
  subquery = make_shared<query_spec>(this, scope);
}

void exists_predicate::accept(prod_visitor *v)
{
  v->visit(this);
  subquery->accept(v);
}

void exists_predicate::out(std::ostream &out)
{
  out << "EXISTS (";
  indent(out);
  out << *subquery << ")";
}

distinct_pred::distinct_pred(prod *p) : bool_binop(p)
{
  lhs = make_shared<column_reference>(this);
  rhs = make_shared<column_reference>(this, lhs->type);
}

comparison_op::comparison_op(prod *p) : bool_binop(p)
{
  auto &idx = p->scope->schema->operators_returning_type;

  auto iters = idx.equal_range(scope->schema->booltype);
  oper = random_pick(random_pick(iters)->second);

  lhs = value_expr::factory(this, oper->left, false);
  rhs = value_expr::factory(this, oper->right, false);

  if (oper->left == oper->right
	 && lhs->type != rhs->type) {

    if (lhs->type->consistent(rhs->type))
      lhs = value_expr::factory(this, rhs->type, false);
    else
      rhs = value_expr::factory(this, lhs->type, false);
  }
}

coalesce::coalesce(prod *p, sqltype *type_constraint, const char *abbrev)
     : value_expr(p), abbrev_(abbrev)
{
  auto first_expr = value_expr::factory(this, type_constraint, false);
  auto second_expr = value_expr::factory(this, first_expr->type, false);

  retry_limit = 20;
  while(first_expr->type != second_expr->type) {
    retry();
    if (first_expr->type->consistent(second_expr->type))
      first_expr = value_expr::factory(this, second_expr->type, false);
    else 
      second_expr = value_expr::factory(this, first_expr->type, false);
  }
  type = second_expr->type;

  value_exprs.push_back(first_expr);
  value_exprs.push_back(second_expr);
}
 
void coalesce::out(std::ostream &out)
{
  out << "cast(" << abbrev_ << "(";
  for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
    out << **expr;
    if (expr+1 != value_exprs.end())
      out << ",", indent(out);
  }
  out << ")";
  out << " as " << type->name << ")";
}

/* Interesting string literals: empty/whitespace, LIKE metacharacters,
   quoting/escaping hazards, unicode of various widths, casts-in-disguise
   and a long string.  All pre-escaped for use in single quotes. */
static const char *interesting_strings[] = {
  "",
  "a",
  "A A",
  " ",
  "  padded  ",
  "0",
  "-1",
  "null",
  "%",
  "_",
  "%_%",
  "\\",
  "''",
  "a''b\\c\"d",
  "ß",
  "áéíóú",
  "日本語",
  "🌍🌍",
  "\U0001D11E",
  "{\"a\": 1}",
  "{1,2,3}",
  "2023-01-01",
  "1 day",
  "inf",
  "NaN",
  "0123456789012345678901234567890123456789",
};

static std::string random_string_literal()
{
  if (d6() < 6) {
    size_t idx = d100() % (sizeof(interesting_strings)/sizeof(*interesting_strings));
    return std::string("'") + interesting_strings[idx] + "'";
  }
  /* random short string over a small alphabet including LIKE
     metacharacters and multi-byte characters */
  static const char *alphabet[] = {"a", "b", "Z", "0", " ", "%", "_", "ß", "''"};
  std::string result = "'";
  int len = d6();
  for (int i = 0; i < len; i++)
    result += alphabet[d9() - 1];
  result += "'";
  return result;
}

const_expr::const_expr(prod *p, sqltype *type_constraint)
    : value_expr(p), expr("")
{
  type = type_constraint ? type_constraint : scope->schema->inttype;

  if (type == scope->schema->inttype)
    expr = to_string(d100());
  else if (type == scope->schema->booltype)
    expr += (d6() > 3) ? scope->schema->true_literal : scope->schema->false_literal;
  // Error: ERROR:  column "default" does not exist
  //else if (dynamic_cast<insert_stmt*>(p) && (d6() > 3))
  //  expr += "default";
  // https://github.com/MaterializeInc/database-issues/issues/5211
  else if (type->name == "any")
    expr = "null";
  else if (type->name == "anycompatible")
    expr = "null";
  /* an untyped literal lets the function/operator overload resolution
     pick a concrete type, an explicit cast to the pseudo type would
     always error */
  else if (type->name == "anynonarray" || type->name == "anyelement"
           || type->name == "anycompatiblenonarray")
    expr = (d6() < 4) ? random_string_literal() : "null";
  else if (type->name == "anyarray")
    expr = "array[null, null]";
  else if (type->name == "anycompatiblearray")
    expr = "array[null, null]";
  else if (type->name == "list")
    expr = "list[null, null]";
  else if (type->name == "anycompatiblelist")
    expr = "list[null, null]";
  else if (type->name == "anyrange")
    expr = "numrange(0,0)";
  else if (type->name == "anycompatiblerange")
    expr = "numrange(0,0)";
  else if (type->name == "record")
    expr = "row(1)";
  else if (type->name == "map")
    expr = "'{}'::map[text=>text]";
  else if (type->name == "anycompatiblemap")
    expr = "'{}'::map[text=>text]";
  else if (type->name == "text[]" || type->name == "_text")
    expr = "array['a', 'b', null, '']::text[]";
  else if (type->name == "smallint[]" || type->name == "_smallint")
    expr = "array[1, 2, 3, null]::smallint[]";
  else if (type->name == "integer[]" || type->name == "_integer")
    expr = "array[1, 2, 3, null]::integer[]";
  else if (type->name == "jsonb")
    if (d6() == 1)
      expr = "'{}'::jsonb";
    else if (d6() == 1)
      expr = "'[]'::jsonb";
    else if (d6() == 1)
      expr = "'\"foo\"'::jsonb";
    else if (d6() == 1)
      expr = "'null'::jsonb";
    else
      expr = "'{\"1\":2,\"3\":4}'::jsonb";
  else if (type->name == "bytea")
    if (d6() == 1)
      expr = "cast('\\000' as bytea)";
    else if (d6() == 1)
      expr = "cast('\\xFFFFFF' as bytea)";
    else
      expr = "cast('\\xDEADBEEF' as bytea)";
  /* cast so the literal is not of type unknown, which polymorphic
     functions reject.  bpchar stays uncast, a bare bpchar cast means
     char(1) and would truncate. */
  else if (type->name == "text" || type->name == "varchar"
           || type->name == "name")
    expr = random_string_literal() + "::" + type->name;
  else if (type->name == "bpchar")
    expr = random_string_literal();
  else if (type->name == "uuid")
    if (d6() == 1)
      expr = "UUID '00000000-0000-0000-0000-000000000000'";
    else if (d6() == 1)
      expr = "UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff'";
    else
      expr = "UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'";
  else if (type->name == "date")
    if (d6() == 1)
      expr = "(DATE '1-11-25' - INTERVAL '4713 YEARS')";
    else if (d6() == 1)
      expr = "(DATE '62143-12-31' + INTERVAL '200000 YEARS')";
    else
      expr = "DATE '2007-02-01'";
  else if (type->name == "time")
    if (d6() == 1)
      expr = "TIME '00:00:00'";
    else if (d6() == 1)
      expr = "TIME '23:59:60'";
    else
      expr = "TIME '01:23:45'";
  else if (type->name == "timestamp")
    if (d6() == 1)
      expr = "(TIMESTAMP '0001-01-01 00:00:00' - INTERVAL '4713 YEARS')";
    else if (d6() == 1)
      expr = "(TIMESTAMP '95143-12-31 23:59:59' + INTERVAL '167 MILLENNIUM')";
    else if (d6() == 1)
      expr = "(TIMESTAMP(0) '0001-01-01 00:00:00' - INTERVAL '4713 YEARS')";
    else if (d6() == 1)
      expr = "(TIMESTAMP(6) '0001-01-01 00:00:00' - INTERVAL '4713 YEARS')";
    else if (d6() == 1)
      expr = "TIMESTAMP(3) '2023-01-01 01:23:45'";
    else
      expr = "TIMESTAMP '2023-01-01 01:23:45'";
  else if (type->name == "timestamptz")
    if (d6() == 1)
      expr = "(TIMESTAMPTZ '0001-01-01 00:00:00+06' - INTERVAL '4713 YEARS')";
    else if (d6() == 1)
      expr = "(TIMESTAMPTZ '95143-12-31 23:59:59+06' + INTERVAL '167 MILLENNIUM')";
    else if (d6() == 1)
      expr = "(TIMESTAMPTZ(0) '0001-01-01 00:00:00+06' - INTERVAL '4713 YEARS')";
    else if (d6() == 1)
      expr = "(TIMESTAMPTZ(6) '95143-12-31 23:59:59+06' + INTERVAL '167 MILLENNIUM')";
    else if (d6() == 1)
      expr = "TIMESTAMPTZ(3) '2023-01-01 01:23:45+06'";
    else
      expr = "TIMESTAMPTZ '2023-01-01 01:23:45+06'";
  else if (type->name == "interval")
    if (d6() == 1)
      expr = "INTERVAL '2147483647 MONTHS'";
    else if (d6() == 1)
      expr = "INTERVAL '-2147483648 MONTHS'";
    else
      expr = "INTERVAL '1' MINUTE";
  else if (type->name == "aclitem")
    expr = "cast(null as aclitem)";
  else if (type->name == "mz_aclitem")
    expr = "cast(null as mz_aclitem)";
  else if (type->name == "oid[]")
    expr = "array[null, 12]::oid[]";
  else if (type->name == "mz_aclitem[]" || type->name == "_mz_aclitem")
    expr = "array[null, null]::mz_aclitem[]";
  else if (type->name == "aclitem[]" || type->name == "_aclitem")
    expr = "array[null, null]::aclitem[]";
  else if (type->name.rfind("[]") == type->name.size() - 3)
    expr = "array[null, null]::" + type->name.substr(type->name.rfind("[]")) + "[]";
  else if (type->name[0] == '_')
    expr = "array[null, null]::" + type->name.substr(1, type->name.size()) + "[]";
  else
    if (d6() < 4) {
      if (type->name == "int2")
        expr = "-32768::int2";
      else if (type->name == "int4")
        expr = "-2147483648::int4";
      else if (type->name == "int8")
        expr = "-9223372036854775808::int8";
      else if (type->name == "uint2")
        expr = "0::uint2";
      else if (type->name == "uint4")
        expr = "0::uint4";
      else if (type->name == "uint8")
        expr = "0::uint8";
      else if (type->name == "float4")
        expr = "'inf'::float4";
      else if (type->name == "float8")
        expr = "'inf'::float8";
      else
        expr = "cast(0 as " + type->name + ")";
    } else if (d6() < 4) {
      if (type->name == "int2")
        expr = "32767::int2";
      else if (type->name == "int4")
        expr = "2147483647::int4";
      else if (type->name == "int8")
        expr = "9223372036854775807::int8";
      else if (type->name == "uint2")
        expr = "65535::uint2";
      else if (type->name == "uint4")
        expr = "4294967295::uint4";
      else if (type->name == "uint8")
        expr = "18446744073709551615::uint8";
      else if (type->name == "float4")
        expr = "'nan'::float4";
      else if (type->name == "float8")
        expr = "'nan'::float8";
      else
        expr = "1::" + type->name;
    } else if (d6() == 1) {
      expr = "2::" + type->name;
    } else if (d6() == 1) {
      expr = "10::" + type->name;
    } else {
      expr = "null::" + type->name;
    }
}

funcall::funcall(prod *p, sqltype *type_constraint, bool can_return_set, bool agg)
  : value_expr(p), is_aggregate(agg)
{
  //if (type_constraint == scope->schema->internaltype)
  //  fail("cannot call functions involving internal type");

  // TODO: pick can_return_set here
  auto &idx = agg ? p->scope->schema->aggregates_returning_type
    : !can_return_set ?
      (4 < d6()) ? p->scope->schema->routines_returning_type_without_returns_set
      : p->scope->schema->parameterless_routines_returning_type_without_returns_set
    : (4 < d6()) ?
    p->scope->schema->routines_returning_type
    : p->scope->schema->parameterless_routines_returning_type;

 retry:
  
  if (!type_constraint) {
    proc = random_pick(random_pick(idx.begin(), idx.end())->second);
  } else {
    auto iters = idx.equal_range(type_constraint);
    proc = random_pick(random_pick(iters)->second);
    if (proc && !type_constraint->consistent(proc->restype)) {
      retry();
      goto retry;
    }
  }

  if (!proc) {
    retry();
    goto retry;
  }

  if (type_constraint)
    type = type_constraint;
  else
    type = proc->restype;

  //if (type == scope->schema->internaltype) {
  //  retry();
  //  goto retry;
  //}

  //for (auto type : proc->argtypes)
  //  if (type == scope->schema->internaltype
  //      || type == scope->schema->arraytype) {
  //    retry();
  //    goto retry;
  //  }
  
  for (auto argtype : proc->argtypes) {
    assert(argtype);
    auto expr = value_expr::factory(this, argtype, false);
    parms.push_back(expr);
  }
}

void funcall::out(std::ostream &out)
{
  out << proc->ident() << "(";
  for (auto expr = parms.begin(); expr != parms.end(); expr++) {
    indent(out);
    // https://github.com/MaterializeInc/database-issues/issues/5211
    if ((*expr)->type->name.rfind("list", 0) != 0 && (*expr)->type->name.rfind("map", 0) != 0 && (*expr)->type->name.rfind("record", 0) != 0 && (*expr)->type->name.rfind("any", 0) != 0)
      out << "CAST(" << **expr << " as " << (*expr)->type->name << ")";
    else
      out << **expr;
    if (expr+1 != parms.end())
      out << ",";
  }

  if (is_aggregate && (parms.begin() == parms.end()))
    out << "*";
  out << ")";
}

opcall::opcall(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  auto &idx = p->scope->schema->operators_returning_type;

  if (!type_constraint) {
    oper = random_pick(random_pick(idx.begin(), idx.end())->second);
  } else {
    auto iters = idx.equal_range(type_constraint);
    oper = random_pick(random_pick(iters)->second);
  }

  if (type_constraint)
    type = type_constraint;
  else
    type = oper->result;

  rhs = value_expr::factory(this, oper->right, false);
  if (oper->left) {
    lhs = value_expr::factory(this, oper->left, false);
  }
  assert(oper->right);
  assert(oper->result);
}

atomic_subselect::atomic_subselect(prod *p, sqltype *type_constraint)
  : value_expr(p), offset((d6() == 6) ? d100() : d6())
{
  match();
  if (d6() < 3) {
    if (type_constraint) {
      auto idx = scope->schema->aggregates_returning_type;
      auto iters = idx.equal_range(type_constraint);
      agg = random_pick(random_pick(iters)->second);
    } else {
      agg = &random_pick<>(scope->schema->aggregates);
    }
    if (agg->argtypes.size() != 1)
      agg = 0;
    else
      type_constraint = agg->argtypes[0];
  } else {
    agg = 0;
  }

  if (type_constraint) {
    auto idx = scope->schema->tables_with_columns_of_type;
    col = 0;
    auto iters = idx.equal_range(type_constraint);
    tab = random_pick(random_pick(iters)->second);

    for (auto &cand : tab->columns()) {
      if (type_constraint->consistent(cand.type)) {
	col = &cand;
	break;
      }
    }
    assert(col);
  } else {
    tab = &random_pick<>(scope->schema->tables);
    col = &random_pick<>(tab->columns());
  }

  type = agg ? agg->restype : col->type;
}

void atomic_subselect::out(std::ostream &out)
{
  out << "(select ";

  if (agg)
    out << agg->ident() << "(" << scope->schema->quote_name(col->name) << ")";
  else
    out << scope->schema->quote_name(col->name);
  
  out << " from " << tab->ident();

  if (!agg)
    out << " limit 1 offset " << offset;

  out << ")";
  indent(out);
}

void window_function::out(std::ostream &out)
{
  indent(out);
  if (aggregate)
    out << *aggregate;
  else {
    out << funcname << "(";
    if (arg)
      out << *arg;
    out << extra_args << ")";
  }
  out << " over (partition by ";

  for (auto ref = partition_by.begin(); ref != partition_by.end(); ref++) {
    out << **ref;
    if (ref+1 != partition_by.end())
      out << ",";
  }

  out << " order by ";

  for (auto ref = order_by.begin(); ref != order_by.end(); ref++) {
    out << **ref;
    if (ref+1 != order_by.end())
      out << ",";
  }

  out << frame;
  out << ")";
}

/* Random window frame clause, with leading space.  Only combinations
   Materialize supports: GROUPS mode and frames mixing an UNBOUNDED
   bound with an offset bound are rejected. */
static std::string random_frame_clause()
{
  if (d6() < 4)
    return "";
  static const char *frames[] = {
    " range between unbounded preceding and current row",
    " rows between unbounded preceding and current row",
    " rows between unbounded preceding and unbounded following",
    " rows between current row and unbounded following",
    " rows between 2 preceding and current row",
    " rows between current row and 3 following",
    " rows between 2 preceding and 3 following",
    " rows between 1 following and 3 following",
    " rows between 3 preceding and 1 preceding",
  };
  return frames[d9() - 1];
}

window_function::window_function(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  match();
  int kind = d12();
  if (kind <= 6) {
    /* aggregate window function */
    aggregate = make_shared<funcall>(this, type_constraint, true, true);
    type = aggregate->type;
    if (!concrete_result_type(type))
      fail("window aggregate with pseudo result type");
    frame = random_frame_clause();
  } else if (kind <= 9) {
    /* ranking window function */
    static const char *ranking[] = {"row_number", "rank", "dense_rank"};
    funcname = ranking[d6() % 3];
    type = scope->schema->types_by_name["int8"];
    if (type_constraint && !type_constraint->consistent(type))
      fail("ranking window function does not match type constraint");
  } else {
    /* value window function */
    static const char *value_funcs[] = {"lag", "lead", "first_value",
                                        "last_value"};
    funcname = value_funcs[d6() % 4];
    arg = value_expr::factory(this, type_constraint, false);
    type = arg->type;
    if (funcname == "lag" || funcname == "lead") {
      if (d6() < 3)
        extra_args = ", " + std::to_string(d6());
    } else {
      frame = random_frame_clause();
    }
  }
  partition_by.push_back(make_shared<column_reference>(this));
  while(d6() > 4)
    partition_by.push_back(make_shared<column_reference>(this));

  order_by.push_back(make_shared<column_reference>(this));
  while(d6() > 4)
    order_by.push_back(make_shared<column_reference>(this));
}

bool window_function::allowed(prod *p)
{
  if (dynamic_cast<select_list *>(p)) {
    /* only in the select list of a query_spec, and not in a grouped
       query where the select list items become grouping keys */
    query_spec *q = dynamic_cast<query_spec *>(p->pprod);
    return q && !q->has_group_by;
  }
  if (dynamic_cast<window_function *>(p))
    return false;
  if (dynamic_cast<value_expr *>(p))
    return allowed(p->pprod);
  return false;
}
