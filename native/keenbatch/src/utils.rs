use rustler::types::tuple::make_tuple;
use rustler::{NifEnv, NifTerm, NifResult, NifEncoder};

pub enum NifResultType {
    Success,
    Fail,
}

pub fn make_result<'a, T: NifEncoder + ?Sized>(env: NifEnv<'a>,
                                               r: NifResultType,
                                               value: &T)
                                               -> NifResult<NifTerm<'a>> {

    let mut tuple = Vec::new();

    match r {
        NifResultType::Success => tuple.push(atoms::ok().encode(env)),
        NifResultType::Fail => tuple.push(atoms::error().encode(env)),
    }
    tuple.push(value.encode(env));
    Ok(make_tuple(env, &tuple))
}

macro_rules! impl_from {
    ($name: ident, $target: ty) => {
        impl From<$target> for $name {
            fn from(arg: $target) -> $name {
                $name(Mutex::new(Some(arg)))
            }
        }
    };

    ($name: ident @ $target: ty) => {
        impl From<$target> for $name {
            fn from(arg: $target) -> $name {
                $name(Mutex::new(arg))
            }
        }
    };
}

macro_rules! fail {
    ($env: expr, $fmt: expr $(,$item: expr)*) => ($crate::utils::make_result($env, $crate::utils::NifResultType::Fail, &*format!($fmt, $($item),*)))
}

macro_rules! succr {
    ($env: expr, $tp: expr, pod) => ($crate::utils::make_result($env, $crate::utils::NifResultType::Success, &ResourceArc::new(PODResultWrapper::from($tp))));
    ($env: expr, $tp: expr, items) => ($crate::utils::make_result($env, $crate::utils::NifResultType::Success, &ResourceArc::new(ItemsResultWrapper::from($tp))));
    ($env: expr, $tp: expr, dayspod) => ($crate::utils::make_result($env, $crate::utils::NifResultType::Success, &ResourceArc::new(DaysPODResultWrapper::from($tp))));
    ($env: expr, $tp: expr, daysitems) => ($crate::utils::make_result($env, $crate::utils::NifResultType::Success, &ResourceArc::new(DaysItemsResultWrapper::from($tp))))
}

macro_rules! delegate {
    ($r:ident . $($tokens: tt)*) => (
        $r.0.lock().unwrap().take().ok_or(NifError::BadArg)?.$($tokens)*
    );
    ($r:ident @ $($tokens: tt)*) => (
        $r.0.lock().unwrap().$($tokens)*
    );
}

pub mod atoms {
    rustler_atoms! {
        atom ok;
        atom error;

        atom eq;
        atom gt;
        atom gte;
        atom lt;
        atom lte;
        atom in_;
        atom ne;

        atom count;
        atom count_unique;

        atom dayspod;
        atom items;
        atom pod;
        atom daysitems;

        atom yearly;
        atom monthly;
        atom weekly;
        atom daily;
        atom hourly;
        atom minutely;
    }
}
