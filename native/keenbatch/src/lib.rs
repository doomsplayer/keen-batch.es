extern crate chrono;
extern crate keenio_batch;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rustler;
#[macro_use]
extern crate rustler_codegen;

#[macro_use]
mod utils;


use rustler::{NifEnv, NifTerm, NifResult, NifEncoder, NifError};
use rustler::resource::ResourceArc;
use rustler::types::atom::NifAtom;
use rustler::types::list::NifListIterator;
use rustler::ErlNifTaskFlags::*;

use keenio_batch::{KeenCacheClient, KeenCacheQuery, KeenCacheResult, Metric, TimeFrame, Filter,
                   Interval, Days, Items, ResultType, ToFilterValue, StringOrI64};
use std::time::Duration;
use std::error::Error;
use std::sync::Mutex;

use utils::{make_result, atoms};
use utils::NifResultType::{Fail, Success};

rustler_export_nifs!("Elixir.KeenBatch",
                     [("new_client", 2, new_client, ERL_NIF_NORMAL_JOB),
                      ("set_redis!", 2, set_redis, ERL_NIF_NORMAL_JOB),
                      ("set_timeout!", 2, set_timeout, ERL_NIF_NORMAL_JOB),
                      ("new_query", 2, new_query, ERL_NIF_NORMAL_JOB),
                      ("group_by!", 2, group_by, ERL_NIF_NORMAL_JOB),
                      ("filter!", 2, filter, ERL_NIF_NORMAL_JOB),
                      ("interval!", 2, interval, ERL_NIF_NORMAL_JOB),
                      ("other!", 3, other, ERL_NIF_NORMAL_JOB),
                      ("accumulate!", 2, accumulate, ERL_NIF_DIRTY_JOB_CPU_BOUND),
                      ("range!", 3, range, ERL_NIF_DIRTY_JOB_CPU_BOUND),
                      ("select!", 4, select, ERL_NIF_DIRTY_JOB_CPU_BOUND),
                      ("send_query", 1, send_query, ERL_NIF_DIRTY_JOB_IO_BOUND),
                      ("to_redis!", 3, to_redis, ERL_NIF_DIRTY_JOB_IO_BOUND),
                      ("from_redis", 3, from_redis, ERL_NIF_DIRTY_JOB_IO_BOUND),
                      ("to_string!", 1, to_string, ERL_NIF_NORMAL_JOB)],
                     Some(on_load));


struct ClientWrapper(Mutex<KeenCacheClient>);
impl_from!(ClientWrapper @ KeenCacheClient);

struct QueryWrapper(Mutex<KeenCacheQuery>);
impl_from!(QueryWrapper @ KeenCacheQuery);

struct PODResultWrapper(Mutex<Option<KeenCacheResult<i64>>>);
impl_from!(PODResultWrapper, KeenCacheResult<i64>);

struct ItemsResultWrapper(Mutex<Option<KeenCacheResult<Items>>>);
impl_from!(ItemsResultWrapper, KeenCacheResult<Items>);

struct DaysPODResultWrapper(Mutex<Option<KeenCacheResult<Days<i64>>>>);
impl_from!(DaysPODResultWrapper, KeenCacheResult<Days<i64>>);

struct DaysItemsResultWrapper(Mutex<Option<KeenCacheResult<Days<Items>>>>);
impl_from!(DaysItemsResultWrapper, KeenCacheResult<Days<Items>>);



// ----------------  apis  -----------------
pub fn on_load(env: NifEnv, _: NifTerm) -> bool {
    resource_struct_init!(ClientWrapper, env);
    resource_struct_init!(QueryWrapper, env);
    resource_struct_init!(PODResultWrapper, env);
    resource_struct_init!(ItemsResultWrapper, env);
    resource_struct_init!(DaysItemsResultWrapper, env);
    resource_struct_init!(DaysPODResultWrapper, env);
    true
}

// key: string, project: string
fn new_client<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let key = args[0].decode()?;
    let project = args[1].decode()?;
    let client: ClientWrapper = KeenCacheClient::new(key, project).into();
    make_result(env, Success, &ResourceArc::new(client))
}

// client: Client, url: string
fn set_redis<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let client: ResourceArc<ClientWrapper> = args[0].decode()?;
    let url = args[1].decode()?;
    let result = delegate!(client@set_redis(url));
    if let Err(e) = result {
        make_result(env, Fail, e.description())
    } else {
        make_result(env, Success, "")
    }
}

// mut c: FFICacheClient, sec: c_int
fn set_timeout<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let client: ResourceArc<ClientWrapper> = args[0].decode()?;
    let sec = args[1].decode()?;
    delegate!(client@set_timeout(Duration::new(sec, 0)));
    Ok(atoms::ok().encode(env))
}

#[derive(NifMap)]
struct QueryOption<'a> {
    pub metric_type: NifTerm<'a>,
    pub metric_target: &'a str,
    pub collection: &'a str,
    pub start: &'a str,
    pub end: &'a str,
}

fn new_query<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let client: ResourceArc<ClientWrapper> = args[0].decode()?;
    let query: QueryOption = args[1].decode()?;
    let metric_type = NifAtom::from_term(query.metric_type)?;

    let metric = match metric_type {
        x if x == atoms::count() => Metric::Count,
        x if x == atoms::count_unique() => Metric::CountUnique(query.metric_target.into()),
        _ => {
            // set_global_error(format!("unsupported metric type").into());
            return Ok(atoms::ok().encode(env));
        }
    };
    let collection = query.collection.to_string();
    let start = match query.start.parse() {
        Ok(u) => u,
        Err(e) => return fail!(env, "{}", e),
    };
    let end = match query.end.parse() {
        Ok(u) => u,
        Err(e) => return fail!(env, "{}", e),
    };
    let query: QueryWrapper =
        delegate!(client@query(metric, collection, TimeFrame::Absolute(start, end))).into();

    Ok(ResourceArc::new(query).encode(env))
}

// mut q: FFICacheQuery, group: *mut c_char
fn group_by<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let query: ResourceArc<QueryWrapper> = args[0].decode()?;
    let group = args[1].decode()?;
    delegate!(query@group_by(group));
    Ok(atoms::ok().encode(env))
}

fn gen_filter<U>(name: &str, value: U, filter_type: NifAtom) -> Result<Filter, String>
    where U: ToFilterValue
{
    let filter = match filter_type {
        x if x == atoms::eq() => Filter::eq(name, value),
        x if x == atoms::lt() => Filter::lt(name, value),
        x if x == atoms::gt() => Filter::gt(name, value),
        x if x == atoms::gte() => Filter::gte(name, value),
        x if x == atoms::lte() => Filter::lte(name, value),
        x if x == atoms::in_() => Filter::isin(name, value),
        x if x == atoms::ne() => Filter::ne(name, value),
        _ => {
            return Err(format!("unsupported filter type"));
        }
    };
    return Ok(filter);
}

#[derive(NifMap)]
struct FilterOptions<'a> {
    pub operator: NifTerm<'a>,
    pub property_name: &'a str,
    pub property_value: NifTerm<'a>,
}

// @param: query
// @param: filter
fn filter<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let query: ResourceArc<QueryWrapper> = args[0].decode()?;

    let filter: FilterOptions = args[1].decode()?;

    let operator = NifAtom::from_term(filter.operator)?;

    let filter = if let Ok(property_value) = filter.property_value.decode::<&str>() {
        gen_filter(filter.property_name, property_value, operator).unwrap()
    } else if let Ok(property_value) = filter.property_value.decode::<i64>() {
        gen_filter(filter.property_name, property_value, operator).unwrap()
    } else {
        let r: NifResult<NifListIterator> = filter.property_value.decode();
        if let Ok(property_value) =
            r.and_then(|v| v.map(|x| x.decode()).collect::<NifResult<Vec<i64>>>()) {
            gen_filter(filter.property_name, property_value, operator).unwrap()
        } else {
            let r: NifResult<NifListIterator> = filter.property_value.decode();
            if let Ok(property_value) =
                r.and_then(|v| v.map(|x| x.decode()).collect::<NifResult<Vec<&str>>>()) {
                gen_filter(filter.property_name, property_value, operator).unwrap()
            } else {
                return fail!(env, "property_value should be rather string or int or list");
            }
        }
    };
    delegate!(query@filter(filter));
    Ok(atoms::ok().encode(env))
}

fn interval<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let query: ResourceArc<QueryWrapper> = args[0].decode()?;
    let interval = NifAtom::from_term(args[1])?;

    match interval {
        x if x == atoms::minutely() => delegate!(query@interval(Interval::Minutely)),
        x if x == atoms::hourly() => delegate!(query@interval(Interval::Hourly)),
        x if x == atoms::daily() => delegate!(query@interval(Interval::Daily)),
        x if x == atoms::weekly() => delegate!(query@interval(Interval::Weekly)),
        x if x == atoms::monthly() => delegate!(query@interval(Interval::Monthly)),
        x if x == atoms::yearly() => delegate!(query@interval(Interval::Yearly)),
        _ => return make_result(env, Fail, "no such interval type"),
    }
    make_result(env, Success, "")
}

fn other<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let query: ResourceArc<QueryWrapper> = args[0].decode()?;
    let key = args[1].decode()?;
    let value = args[2].decode()?;
    delegate!(query@other(key, value));
    make_result(env, Success, "")
}

fn send_query<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let query: ResourceArc<QueryWrapper> = args[0].decode()?;
    let tp = delegate!(query@tp);
    match tp {
        ResultType::POD => {
            match delegate!(query@data()) {
                Ok(s) => make_result(env, Success, &ResourceArc::new(PODResultWrapper::from(s))),
                Err(e) => fail!(env, "data type can not be converted to i64: '{}'", e),
            }
        }
        ResultType::Items => {
            match delegate!(query@data()) {
                Ok(s) => make_result(env, Success, &ResourceArc::new(ItemsResultWrapper::from(s))),
                Err(e) => fail!(env, "data type can not be converted to Items: '{}'", e),
            }
        }
        ResultType::DaysPOD => {
            match delegate!(query@data()) {
                Ok(s) => {
                    make_result(env,
                                Success,
                                &ResourceArc::new(DaysPODResultWrapper::from(s)))
                }
                Err(e) => fail!(env, "data type can not be converted to Days<i64>: '{}'", e),
            }
        }
        ResultType::DaysItems => {
            match delegate!(query@data()) {
                Ok(s) => {
                    make_result(env,
                                Success,
                                &ResourceArc::new(DaysItemsResultWrapper::from(s)))
                }
                Err(e) => fail!(env, "data type can not be converted to Days<Items>: '{}'", e),
            }
        }
    }
}

fn accumulate<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let to = NifAtom::from_term(args[1])?;

    if let Ok(_) = args[0].decode::<ResourceArc<PODResultWrapper>>() {
        fail!(env, "i64 can not be converted to others")
    } else if let Ok(r) = args[0].decode::<ResourceArc<ItemsResultWrapper>>() {
        let s = delegate!(r.accumulate());
        make_result(env, Success, &ResourceArc::new(PODResultWrapper::from(s)))
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysPODResultWrapper>>() {
        let s = delegate!(r.accumulate());
        make_result(env, Success, &ResourceArc::new(PODResultWrapper::from(s)))
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysItemsResultWrapper>>() {
        match to {
            x if x == atoms::dayspod() => {
                let s = delegate!(r.accumulate());
                make_result(env,
                            Success,
                            &ResourceArc::new(DaysPODResultWrapper::from(s)))
            }
            x if x == atoms::pod() => {
                let s = delegate!(r.accumulate());
                make_result(env, Success, &ResourceArc::new(PODResultWrapper::from(s)))
            }
            _ => make_result(env, Fail, &*format!("data type can not be converted to")),
        }
    } else {
        make_result(env, Fail, &*format!("not a valid target type"))
    }
}

fn range<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let from = match try!(args[1].decode::<&str>()).parse() {
        Ok(u) => u,
        Err(e) => return fail!(env, "{}", e),
    };
    let to = match try!(args[2].decode::<&str>()).parse() {
        Ok(u) => u,
        Err(e) => return fail!(env, "{}", e),
    };

    if let Ok(_) = args[0].decode::<ResourceArc<PODResultWrapper>>() {
        fail!(env, "i64 can not be converted to others")
    } else if let Ok(_) = args[0].decode::<ResourceArc<ItemsResultWrapper>>() {
        fail!(env, "Items can not be converted to others")
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysPODResultWrapper>>() {
        let r = delegate!(r.range(from, to));
        succr!(env, r, dayspod)
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysItemsResultWrapper>>() {
        let r = delegate!(r.range(from, to));
        succr!(env, r, daysitems)
    } else {
        fail!(env, "not a valid source type")
    }
}

fn select<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {

    let key = args[1].decode()?;
    let value: i64 = args[2].decode()?;
    let to = NifAtom::from_term(args[3])?;

    if let Ok(_) = args[0].decode::<ResourceArc<PODResultWrapper>>() {
        fail!(env, "i64 not support select")
    } else if let Ok(r) = args[0].decode::<ResourceArc<ItemsResultWrapper>>() {
        match to {
            x if x == atoms::daysitems() || x == atoms::dayspod() => {
                fail!(env, "Items can not be converted to Days<Items> or Days<i64>")
            }
            x if x == atoms::items() => {
                let r = delegate!(r.select((key, StringOrI64::I64(value))));
                succr!(env, r, items)
            }
            x if x == atoms::pod() => {
                let r = delegate!(r.select((key, StringOrI64::I64(value))));
                succr!(env, r, pod)
            }
            _ => fail!(env, "not a valid target type"),
        }
    } else if let Ok(_) = args[0].decode::<ResourceArc<DaysPODResultWrapper>>() {
        fail!(env, "i64 not support select")
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysItemsResultWrapper>>() {
        let param = (key, StringOrI64::I64(value.into()));
        match to {
            x if x == atoms::daysitems() => succr!(env, delegate!(r.select(param)), daysitems),
            x if x == atoms::pod() => succr!(env, delegate!(r.select(param)), pod),
            x if x == atoms::dayspod() => succr!(env, delegate!(r.select(param)), dayspod),
            _ => fail!(env, "not a valid target type"),
        }
    } else {
        fail!(env, "not a valid source type")
    }
}

fn to_redis<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {

    let expire = args[2].decode()?;
    let key = args[1].decode()?;

    let result = if let Ok(r) = args[0].decode::<ResourceArc<PODResultWrapper>>() {
        delegate!(r.to_redis(key, expire))
    } else if let Ok(r) = args[0].decode::<ResourceArc<ItemsResultWrapper>>() {
        delegate!(r.to_redis(key, expire))
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysPODResultWrapper>>() {
        delegate!(r.to_redis(key, expire))
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysItemsResultWrapper>>() {
        delegate!(r.to_redis(key, expire))
    } else {
        return fail!(env, "not a valid source type");
    };
    match result {
        Ok(_) => make_result(env, Success, ""),
        Err(e) => fail!(env, "{}", e),
    }
}

fn to_string<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let s: String = if let Ok(r) = args[0].decode::<ResourceArc<PODResultWrapper>>() {
        delegate!(r.to_string())
    } else if let Ok(r) = args[0].decode::<ResourceArc<ItemsResultWrapper>>() {
        delegate!(r.to_string())
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysPODResultWrapper>>() {
        delegate!(r.to_string())
    } else if let Ok(r) = args[0].decode::<ResourceArc<DaysItemsResultWrapper>>() {
        delegate!(r.to_string())
    } else {
        return fail!(env, "not a valid source type");
    };
    make_result(env, Success, &*s)
}

fn from_redis<'a>(env: NifEnv<'a>, args: &[NifTerm<'a>]) -> NifResult<NifTerm<'a>> {
    let key = args[1].decode()?;
    let url = args[0].decode()?;
    let tp = NifAtom::from_term(args[2])?;

    macro_rules! from_redis {
        ($env: expr, $t: ident) => {
            match KeenCacheResult::from_redis(url, key) {
                Ok(o) => succr!($env, o, $t),
                Err(e) => fail!($env, "{}", e)
            }
        }
    }

    match tp {
        x if x == atoms::pod() => from_redis!(env, pod),
        x if x == atoms::items() => from_redis!(env, items),
        x if x == atoms::dayspod() => from_redis!(env, dayspod),
        x if x == atoms::daysitems() => from_redis!(env, daysitems),
        _ => fail!(env, "not a valid target type"),
    }
}
