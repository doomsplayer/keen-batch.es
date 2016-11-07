
use rustler::{NifEnv, NifTerm, NifResult, NifEncoder, NifError};
use rustler::resource::ResourceCell;
use rustler::atom::{get_atom_init, NifAtom};
use rustler::list::NifListIterator;
use rustler::tuple::make_tuple;
use rustler::wrapper::nif_interface::ErlNifTaskFlags::*;

use keenio_batch::{KeenCacheClient, KeenCacheQuery, KeenCacheResult, Metric, TimeFrame, Filter,
                   Interval, Days, Items, ResultType, ToFilterValue, StringOrI64};
use std::time::Duration;
use std::error::Error;
use std::ops::{Deref, DerefMut};

rustler_export_nifs!("Elixir.KeenBatch",
                     [("new_client", 2, new_client, ERL_NIF_NORMAL_JOB),
                      ("set_redis!", 2, set_redis, ERL_NIF_NORMAL_JOB),
                      ("set_timeout!", 2, set_timeout, ERL_NIF_NORMAL_JOB),
                      ("new_query", 2, new_query, ERL_NIF_NORMAL_JOB),
                      ("group_by!", 2, group_by, ERL_NIF_NORMAL_JOB),
                      ("filter!", 2, filter, ERL_NIF_NORMAL_JOB),
                      ("interval!", 2, interval, ERL_NIF_NORMAL_JOB),
                      ("other!", 3, other, ERL_NIF_NORMAL_JOB),
                      ("accumulate!", 2, accumulate, ERL_NIF_NORMAL_JOB),
                      ("range!", 3, range, ERL_NIF_NORMAL_JOB),
                      ("select!", 4, select, ERL_NIF_NORMAL_JOB),
                      ("send_query", 1, send_query, ERL_NIF_NORMAL_JOB),
                      ("to_redis!", 3, to_redis, ERL_NIF_NORMAL_JOB),
                      ("from_redis", 3, from_redis, ERL_NIF_NORMAL_JOB),
                      ("to_string!", 1, to_string, ERL_NIF_NORMAL_JOB)],
                     Some(on_load));

macro_rules! impl_wrapper {
    ($name: ident, $target: ty) => {
        impl Deref for $name {
            type Target = $target;
            fn deref(&self) -> &$target {
                &self.0
            }
        }
        impl DerefMut for $name {
            fn deref_mut(&mut self) -> &mut $target {
                &mut self.0
            }
        }
        impl From<$target> for $name {
            fn from(arg: $target) -> $name {
                $name(arg)
            }
        }
    }
}

macro_rules! fail {
    ($env: expr, $fmt: expr $(,$item: expr)*) => (make_result($env, Fail, &*format!($fmt, $($item),*)))
}

macro_rules! succr {
    ($env: expr, $tp: expr, pod) => (make_result($env, Success, &ResourceCell::new(PODResultWrapper(Some($tp)))));
    ($env: expr, $tp: expr, items) => (make_result($env, Success, &ResourceCell::new(ItemsResultWrapper(Some($tp)))));
    ($env: expr, $tp: expr, dayspod) => (make_result($env, Success, &ResourceCell::new(DaysPODResultWrapper(Some($tp)))));
    ($env: expr, $tp: expr, daysitems) => (make_result($env, Success, &ResourceCell::new(DaysItemsResultWrapper(Some($tp)))))
}

#[NifResource]
struct ClientWrapper(KeenCacheClient);
impl_wrapper!(ClientWrapper, KeenCacheClient);

#[NifResource]
struct QueryWrapper(KeenCacheQuery);
impl_wrapper!(QueryWrapper, KeenCacheQuery);


#[NifResource]
struct PODResultWrapper(Option<KeenCacheResult<i64>>);
impl_wrapper!(PODResultWrapper, Option<KeenCacheResult<i64>>);

#[NifResource]
struct ItemsResultWrapper(Option<KeenCacheResult<Items>>);
impl_wrapper!(ItemsResultWrapper, Option<KeenCacheResult<Items>>);

#[NifResource]
struct DaysPODResultWrapper(Option<KeenCacheResult<Days<i64>>>);
impl_wrapper!(DaysPODResultWrapper, Option<KeenCacheResult<Days<i64>>>);

#[NifResource]
struct DaysItemsResultWrapper(Option<KeenCacheResult<Days<Items>>>);
impl_wrapper!(DaysItemsResultWrapper, Option<KeenCacheResult<Days<Items>>>);


enum NifResultType {
    Success,
    Fail,
}
use NifResultType::*;

fn make_result<'a, T: NifEncoder + ?Sized>(env: &'a NifEnv,
                                           r: NifResultType,
                                           value: &T)
                                           -> NifResult<NifTerm<'a>> {

    let mut tuple = Vec::new();

    match r {
        Success => tuple.push(get_atom_init("ok").to_term(env)),
        Fail => tuple.push(get_atom_init("error").to_term(env)),
    }
    tuple.push(value.encode(env));
    Ok(make_tuple(env, &tuple))
}

// ----------------  apis  -----------------
pub fn on_load(env: &NifEnv, _: NifTerm) -> bool {
    resource_struct_init!(ClientWrapper, env);
    resource_struct_init!(QueryWrapper, env);
    resource_struct_init!(PODResultWrapper, env);
    resource_struct_init!(ItemsResultWrapper, env);
    resource_struct_init!(DaysItemsResultWrapper, env);
    resource_struct_init!(DaysPODResultWrapper, env);
    true
}

// key: string, project: string
fn new_client<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let key = try!(args[0].decode());
    let project = try!(args[1].decode());
    let client: ClientWrapper = KeenCacheClient::new(key, project).into();
    make_result(env, Success, &ResourceCell::new(client))
}

// client: Client, url: string
fn set_redis<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let client: ResourceCell<ClientWrapper> = try!(args[0].decode());
    let url = try!(args[1].decode());
    let mut client = client.write().unwrap();
    if let Err(e) = client.set_redis(url) {
        make_result(env, Fail, e.description())
    } else {
        make_result(env, Success, "")
    }
}

// mut c: FFICacheClient, sec: c_int
fn set_timeout<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let client: ResourceCell<ClientWrapper> = try!(args[0].decode());
    let sec = try!(args[1].decode());
    client.write().unwrap().set_timeout(Duration::new(sec, 0));
    Ok(get_atom_init("ok").to_term(env))
}

#[ExStruct(module = "Elixir.KeenBatch.QueryOption")]
struct QueryOption<'a> {
    pub metric_type: NifTerm<'a>,
    pub metric_target: &'a str,
    pub collection: &'a str,
    pub start: &'a str,
    pub end: &'a str,
}

fn new_query<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let client: ResourceCell<ClientWrapper> = try!(args[0].decode());
    let query: QueryOption = try!(args[1].decode());
    let metric_type = try!(NifAtom::from_term(env, query.metric_type).ok_or(NifError::BadArg));

    let metric = match metric_type {
        x if x == get_atom_init("count") => Metric::Count,
        x if x == get_atom_init("count_unique") => Metric::CountUnique(query.metric_target.into()),
        _ => {
            // set_global_error(format!("unsupported metric type").into());
            return Ok(get_atom_init("ok").to_term(env));
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
        client.read().unwrap().query(metric, collection, TimeFrame::Absolute(start, end)).into();

    Ok(ResourceCell::new(query).encode(env))
}

// mut q: FFICacheQuery, group: *mut c_char
fn group_by<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let query: ResourceCell<QueryWrapper> = try!(args[0].decode());
    let group = try!(args[1].decode());
    query.write().unwrap().group_by(group);
    Ok(get_atom_init("ok").to_term(env))
}

fn gen_filter<U>(name: &str, value: U, filter_type: NifAtom) -> Result<Filter, String>
    where U: ToFilterValue
{
    let filter = match filter_type {
        x if x == get_atom_init("eq") => Filter::eq(name, value),
        x if x == get_atom_init("lt") => Filter::lt(name, value),
        x if x == get_atom_init("gt") => Filter::gt(name, value),
        x if x == get_atom_init("gte") => Filter::gte(name, value),
        x if x == get_atom_init("lte") => Filter::lte(name, value),
        x if x == get_atom_init("in") => Filter::isin(name, value),
        x if x == get_atom_init("ne") => Filter::ne(name, value),
        _ => {
            return Err(format!("unsupported filter type"));
        }
    };
    return Ok(filter);
}

#[ExStruct(module = "Elixir.KeenBatch.Filter")]
struct FilterOptions<'a> {
    pub operator: NifTerm<'a>,
    pub property_name: &'a str,
    pub property_value: NifTerm<'a>,
}

// @param: query
// @param: filter
fn filter<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let query: ResourceCell<QueryWrapper> = try!(args[0].decode());

    let filter: FilterOptions = try!(args[1].decode());

    let operator = try!(NifAtom::from_term(env, filter.operator).ok_or(NifError::BadArg));

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
    query.write().unwrap().filter(filter);
    Ok(get_atom_init("ok").to_term(env))
}

fn interval<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let query: ResourceCell<QueryWrapper> = try!(args[0].decode());
    let interval = try!(NifAtom::from_term(env, args[1]).ok_or(NifError::BadArg));

    match interval {
        x if x == get_atom_init("minutely") => query.write().unwrap().interval(Interval::Minutely),
        x if x == get_atom_init("hourly") => query.write().unwrap().interval(Interval::Hourly),
        x if x == get_atom_init("daily") => query.write().unwrap().interval(Interval::Daily),
        x if x == get_atom_init("weekly") => query.write().unwrap().interval(Interval::Weekly),
        x if x == get_atom_init("monthly") => query.write().unwrap().interval(Interval::Monthly),
        x if x == get_atom_init("yearly") => query.write().unwrap().interval(Interval::Yearly),
        _ => return make_result(env, Fail, "no such interval type"),
    }
    make_result(env, Success, "")
}

fn other<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let query: ResourceCell<QueryWrapper> = try!(args[0].decode());
    let key = try!(args[1].decode());
    let value = try!(args[2].decode());
    query.write().unwrap().other(key, value);
    make_result(env, Success, "")
}

fn send_query<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let query: ResourceCell<QueryWrapper> = try!(args[0].decode());
    let query = query.read().unwrap();

    match query.tp {
        ResultType::POD => {
            match query.data() {
                Ok(s) => make_result(env, Success, &ResourceCell::new(PODResultWrapper(Some(s)))),
                Err(e) => fail!(env, "data type can not be converted to i64: '{}'", e),
            }
        }
        ResultType::Items => {
            match query.data() {
                Ok(s) => {
                    make_result(env,
                                Success,
                                &ResourceCell::new(ItemsResultWrapper(Some(s))))
                }
                Err(e) => fail!(env, "data type can not be converted to Items: '{}'", e),
            }
        }
        ResultType::DaysPOD => {
            match query.data() {
                Ok(s) => {
                    make_result(env,
                                Success,
                                &ResourceCell::new(DaysPODResultWrapper(Some(s))))
                }
                Err(e) => fail!(env, "data type can not be converted to Days<i64>: '{}'", e),
            }
        }
        ResultType::DaysItems => {
            match query.data() {
                Ok(s) => {
                    make_result(env,
                                Success,
                                &ResourceCell::new(DaysItemsResultWrapper(Some(s))))
                }
                Err(e) => fail!(env, "data type can not be converted to Days<Items>: '{}'", e),
            }
        }
    }
}

fn accumulate<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let to = NifAtom::from_term(env, args[1]).unwrap();

    if let Ok(_) = args[0].decode::<ResourceCell<PODResultWrapper>>() {
        fail!(env, "i64 can not be converted to others")
    } else if let Ok(r) = args[0].decode::<ResourceCell<ItemsResultWrapper>>() {
        let s = try!(r.write().unwrap().take().ok_or(NifError::BadArg)).accumulate();
        make_result(env, Success, &ResourceCell::new(PODResultWrapper(Some(s))))
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysPODResultWrapper>>() {
        let s = try!(r.write().unwrap().take().ok_or(NifError::BadArg)).accumulate();
        make_result(env, Success, &ResourceCell::new(PODResultWrapper(Some(s))))
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysItemsResultWrapper>>() {
        match to {
            x if x == get_atom_init("dayspod") => {
                let s = try!(r.write().unwrap().take().ok_or(NifError::BadArg)).accumulate();
                make_result(env,
                            Success,
                            &ResourceCell::new(DaysPODResultWrapper(Some(s))))
            }
            x if x == get_atom_init("pod") => {
                let s = try!(r.write().unwrap().take().ok_or(NifError::BadArg)).accumulate();
                make_result(env, Success, &ResourceCell::new(PODResultWrapper(Some(s))))
            }
            _ => make_result(env, Fail, &*format!("data type can not be converted to")),
        }
    } else {
        make_result(env, Fail, &*format!("not a valid target type"))
    }
}

fn range<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let from = match try!(args[1].decode::<&str>()).parse() {
        Ok(u) => u,
        Err(e) => return fail!(env, "{}", e),
    };
    let to = match try!(args[2].decode::<&str>()).parse() {
        Ok(u) => u,
        Err(e) => return fail!(env, "{}", e),
    };

    if let Ok(_) = args[0].decode::<ResourceCell<PODResultWrapper>>() {
        fail!(env, "i64 can not be converted to others")
    } else if let Ok(_) = args[0].decode::<ResourceCell<ItemsResultWrapper>>() {
        fail!(env, "Items can not be converted to others")
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysPODResultWrapper>>() {
        let r = try!(r.write().unwrap().take().ok_or(NifError::BadArg)).range(from, to);
        succr!(env, r, dayspod)
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysItemsResultWrapper>>() {
        let r = try!(r.write().unwrap().take().ok_or(NifError::BadArg)).range(from, to);
        succr!(env, r, daysitems)
    } else {
        fail!(env, "not a valid source type")
    }
}

fn select<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {

    let key = try!(args[1].decode());
    let value: i64 = try!(args[2].decode());
    let to = try!(NifAtom::from_term(env, args[3]).ok_or(NifError::BadArg));

    if let Ok(_) = args[0].decode::<ResourceCell<PODResultWrapper>>() {
        fail!(env, "i64 not support select")
    } else if let Ok(r) = args[0].decode::<ResourceCell<ItemsResultWrapper>>() {
        match to {
            x if x == get_atom_init("daysitems") || x == get_atom_init("dayspod") => {
                fail!(env, "Items can not be converted to Days<Items> or Days<i64>")
            }
            x if x == get_atom_init("items") => {
                let r = try!(r.write()
                    .unwrap()
                    .take()
                    .ok_or(NifError::BadArg))
                    .select((key, StringOrI64::I64(value)));
                succr!(env, r, items)
            }
            x if x == get_atom_init("pod") => {
                let r = try!(r.write()
                    .unwrap()
                    .take()
                    .ok_or(NifError::BadArg))
                    .select((key, StringOrI64::I64(value)));
                succr!(env, r, pod)
            }
            _ => fail!(env, "not a valid target type"),
        }
    } else if let Ok(_) = args[0].decode::<ResourceCell<DaysPODResultWrapper>>() {
        fail!(env, "i64 not support select")
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysItemsResultWrapper>>() {
        let r = try!(r.write().unwrap().take().ok_or(NifError::BadArg));
        let param = (key, StringOrI64::I64(value.into()));
        match to {
            x if x == get_atom_init("daysitems") => succr!(env, r.select(param), daysitems),
            x if x == get_atom_init("pod") => succr!(env, r.select(param), pod),
            x if x == get_atom_init("dayspod") => succr!(env, r.select(param), dayspod),
            _ => fail!(env, "not a valid target type"),
        }
    } else {
        fail!(env, "not a valid source type")
    }
}

fn to_redis<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {

    let expire = try!(args[2].decode());
    let key = try!(args[1].decode());

    let result = if let Ok(r) = args[0].decode::<ResourceCell<PODResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_redis(key, expire)
    } else if let Ok(r) = args[0].decode::<ResourceCell<ItemsResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_redis(key, expire)
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysPODResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_redis(key, expire)
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysItemsResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_redis(key, expire)
    } else {
        return fail!(env, "not a valid source type");
    };
    match result {
        Ok(_) => make_result(env, Success, ""),
        Err(e) => fail!(env, "{}", e),
    }
}

fn to_string<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let s = if let Ok(r) = args[0].decode::<ResourceCell<PODResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_string()
    } else if let Ok(r) = args[0].decode::<ResourceCell<ItemsResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_string()
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysPODResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_string()
    } else if let Ok(r) = args[0].decode::<ResourceCell<DaysItemsResultWrapper>>() {
        try!(r.write().unwrap().take().ok_or(NifError::BadArg)).to_string()
    } else {
        return fail!(env, "not a valid source type");
    };
    make_result(env, Success, &*s)
}

fn from_redis<'a>(env: &'a NifEnv, args: &Vec<NifTerm>) -> NifResult<NifTerm<'a>> {
    let key = try!(args[1].decode());
    let url = try!(args[0].decode());
    let tp = try!(NifAtom::from_term(env, args[2]).ok_or(NifError::BadArg));

    macro_rules! from_redis {
        ($env: expr, $t: ident) => {
            match KeenCacheResult::from_redis(url, key) {
                Ok(o) => succr!($env, o, $t),
                Err(e) => fail!($env, "{}", e)
            }
        }
    }

    match tp {
        x if x == get_atom_init("pod") => from_redis!(env, pod),
        x if x == get_atom_init("items") => from_redis!(env, items),
        x if x == get_atom_init("dayspod") => from_redis!(env, dayspod),
        x if x == get_atom_init("daysitems") => from_redis!(env, daysitems),
        _ => fail!(env, "not a valid target type"),
    }
}
