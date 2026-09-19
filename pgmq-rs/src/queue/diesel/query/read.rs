use crate::queue::diesel::sql::PgMessage;
use crate::types::message::{all_columns, msg_id, read_ct};
use crate::types::{QueueName, VisibilityTimeoutOffset};
use crate::Message;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::serialize::ToSql;
use diesel::sql_types::*;
use diesel::{QueryResult, Selectable, SelectableHelper};
// Todo: Macro to generate all of this?
// Todo: Custom `sql_function` macro to use named parameters
// Todo: Custom `select` to add `FROM` clause and specific columns?

#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub struct pgmq_read<Q, VT, Qty> {
    queue_name: Q,
    vt: VT,
    qty: Qty,
}

pub fn pgmq_read<Q, VT, Qty>(queue_name: Q, vt: VT, qty: Qty) -> pgmq_read<Q, VT, Qty>
where
    Q: ToSql<Text, Pg>,
    VT: ToSql<Integer, Pg>,
    Qty: ToSql<Integer, Pg>,
{
    pgmq_read {
        queue_name,
        vt,
        qty,
    }
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    pgmq_read(queue_name, visibility_timeout, quantity)
}

impl<Q, VT, Qty> QueryId for pgmq_read<Q, VT, Qty> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Q, VT, Qty> Query for pgmq_read<Q, VT, Qty> {
    type SqlType = PgMessage;
}

impl<Q, VT, Qty, C> diesel::RunQueryDsl<C> for pgmq_read<Q, VT, Qty> where
    C: diesel::connection::LoadConnection<Backend = Pg>
{
}

impl<Q, VT, Qty> QueryFragment<Pg> for pgmq_read<Q, VT, Qty>
where
    Q: ToSql<Text, Pg>,
    VT: ToSql<Integer, Pg>,
    Qty: ToSql<Integer, Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        // Todo: Somehow allow calling ".select" on a function struct to allow each invocation to
        //  configure which fields to select instead of always selecting all fields.
        out.push_sql("SELECT ");

        // Todo: statically generate the list of fields to select
        out.push_sql("(");
        all_columns.walk_ast(out.reborrow())?;
        out.push_sql(") ");

        out.push_sql("FROM ");

        // Todo: Implement `FunctionFragment` and do `<Self as FunctionFragment<#backend>>::FUNCTION_NAME` instead?
        out.push_sql("pgmq.read");

        out.push_sql("(");

        // Todo: statically generate the parameters (allows changing parameter order in the SQL without breaking the rust client)
        out.push_sql("queue_name=>");
        out.push_bind_param::<Text, _>(&self.queue_name)?;

        out.push_sql(", ");

        out.push_sql("vt=>");
        out.push_bind_param::<Integer, _>(&self.vt)?;

        out.push_sql(", ");

        out.push_sql("qty=>");
        out.push_bind_param::<Integer, _>(&self.qty)?;

        out.push_sql(")");
        Ok(())
    }
}
