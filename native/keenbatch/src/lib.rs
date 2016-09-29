// When writing your NIF you should edit `lib.rs.in`. This is done
// to support both compilation with and without syntex.

// Enable the rustler_codegen compiler plugin if we are not using
// syntex.
#![cfg_attr(not(feature = "with-syntex"), feature(plugin, custom_attribute))]
#![cfg_attr(not(feature = "with-syntex"), plugin(rustler_codegen))]

#![feature(link_args)]
#[cfg(target_os="macos")]
#[link_args = "-flat_namespace -undefined suppress"]
extern "C" {}

extern crate chrono;
extern crate keenio_batch;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rustler;

// If we are using syntex, include the expanded file. This is what
// causes bad error messages.
#[cfg(feature = "with-syntex")]
include!(concat!(env!("OUT_DIR"), "/lib.rs"));

// If we are on nightly, we include the file directly.
// The rust compiler will then report errors in that file.
#[cfg(not(feature = "with-syntex"))]
include!("lib.in.rs");
