// SPDX-License-Identifier: MIT OR Apache-2.0
//! Proc-macro implementation for deriving `auto_irokle::AutoIrokle`.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::parse_quote;
use syn::spanned::Spanned;
use syn::{
    Data, DeriveInput, Field, Fields, GenericArgument, LitStr, Path, PathArguments, Token, Type,
    parse_macro_input,
};

#[proc_macro_derive(AutoIrokle, attributes(auto_irokle))]
pub fn derive_auto_irokle(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand_auto_irokle(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

struct StructArgs {
    entries: Vec<StructArg>,
}

enum StructArg {
    TypeId(LitStr),
    Crate(LitStr),
}

impl Parse for StructArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut entries = Vec::new();

        while !input.is_empty() {
            let key = syn::Ident::parse_any(input)?;
            input.parse::<Token![=]>()?;

            if key == "type_id" {
                entries.push(StructArg::TypeId(input.parse()?));
            } else if key == "crate" {
                entries.push(StructArg::Crate(input.parse()?));
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unsupported struct-level auto_irokle attribute; expected `type_id` or `crate`",
                ));
            }

            if input.is_empty() {
                break;
            }

            input.parse::<Token![,]>()?;
        }

        Ok(Self { entries })
    }
}

struct FieldArgs {
    kind: Option<KindOverride>,
}

#[derive(Clone, Copy)]
enum KindOverride {
    Lww,
    Set,
    Map,
}

impl Parse for FieldArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut kind = None;

        while !input.is_empty() {
            let key = syn::Ident::parse_any(input)?;
            input.parse::<Token![=]>()?;

            if key == "kind" {
                let value: LitStr = input.parse()?;
                let parsed = match value.value().as_str() {
                    "lww" => KindOverride::Lww,
                    "set" => KindOverride::Set,
                    "map" => KindOverride::Map,
                    other => {
                        return Err(syn::Error::new(
                            value.span(),
                            format!(
                                "auto_irokle `kind` must be one of \"lww\", \"set\", or \"map\"; got {other:?}"
                            ),
                        ));
                    }
                };
                if kind.is_some() {
                    return Err(syn::Error::new(
                        value.span(),
                        "duplicate auto_irokle `kind` attribute",
                    ));
                }
                kind = Some(parsed);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unsupported field-level auto_irokle attribute; expected `kind`",
                ));
            }

            if input.is_empty() {
                break;
            }

            input.parse::<Token![,]>()?;
        }

        Ok(Self { kind })
    }
}

struct Config {
    event_type_id: proc_macro2::TokenStream,
    type_label: proc_macro2::TokenStream,
    crate_path: Path,
}

enum FieldKind {
    Lww,
    Set { item: Box<Type> },
    Map { key: Box<Type>, value: Box<Type> },
}

struct AutoField<'a> {
    ident: &'a syn::Ident,
    name: String,
    kind: FieldKind,
}

fn expand_auto_irokle(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let config = parse_config(input)?;
    let ident = &input.ident;
    let fields = parse_fields(input)?;
    let crate_path = &config.crate_path;

    let diff_fields = fields.iter().map(|field| diff_field(crate_path, field));
    let init_fields = fields.iter().map(|field| init_field(crate_path, field));
    let apply_arms = fields
        .iter()
        .flat_map(|field| apply_field(crate_path, field));

    let mut generics = input.generics.clone();
    generics.make_where_clause().predicates.push(parse_quote!(
        Self: Clone
            + PartialEq
            + ::serde::Serialize
            + ::serde::de::DeserializeOwned
            + Send
            + Sync
            + 'static
    ));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let event_type_id = &config.event_type_id;
    let type_label = &config.type_label;

    Ok(quote! {
        impl #impl_generics #crate_path::AutoIrokle for #ident #type_generics #where_clause {
            const EVENT_TYPE_ID: &'static str = #event_type_id;

            fn diff(old: &Self, new: &Self) -> #crate_path::irokle::Result<Vec<#crate_path::PatchOp>> {
                let mut ops = Vec::new();
                #(#diff_fields)*
                Ok(ops)
            }

            fn apply_init(
                projection: &mut #crate_path::AutoProjection<Self>,
                value: Self,
                meta: &#crate_path::__private::OpMeta,
            ) -> #crate_path::irokle::Result<()> {
                if projection.state_opt().is_some() {
                    #crate_path::__private::log_replayed_init(#type_label);
                    return Ok(());
                }
                projection.replace_state(value);
                #(#init_fields)*
                Ok(())
            }

            fn apply_patch_op(
                projection: &mut #crate_path::AutoProjection<Self>,
                op: &#crate_path::PatchOp,
                meta: &#crate_path::__private::OpMeta,
            ) -> #crate_path::irokle::Result<()> {
                match op {
                    #(#apply_arms)*
                    _ => {
                        #crate_path::__private::log_unsupported_patch_op(#type_label);
                        Err(#crate_path::irokle::Error::Decode(format!(
                            "unsupported auto-irokle patch op for {}",
                            #type_label,
                        )))
                    }
                }
            }
        }
    })
}

fn parse_config(input: &DeriveInput) -> syn::Result<Config> {
    let mut type_id = None;
    let mut crate_path = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("auto_irokle") {
            continue;
        }

        let args = attr.parse_args::<StructArgs>()?;
        for entry in args.entries {
            match entry {
                StructArg::TypeId(value) => {
                    if type_id.is_some() {
                        return Err(syn::Error::new(
                            value.span(),
                            "duplicate auto_irokle `type_id` attribute",
                        ));
                    }
                    validate_type_id(&value)?;
                    type_id = Some(value);
                }
                StructArg::Crate(value) => {
                    if crate_path.is_some() {
                        return Err(syn::Error::new(
                            value.span(),
                            "duplicate auto_irokle `crate` attribute",
                        ));
                    }
                    crate_path = Some(value.parse::<Path>().map_err(|_| {
                        syn::Error::new(value.span(), "auto_irokle `crate` must be a Rust path")
                    })?);
                }
            }
        }
    }

    let ident = &input.ident;
    let (event_type_id, type_label) = match type_id {
        Some(type_id) => {
            let event_type_id = LitStr::new(
                &format!("{}/auto-patch.v1", type_id.value()),
                type_id.span(),
            );
            (quote!(#event_type_id), quote!(#type_id))
        }
        None => (
            quote!(concat!(
                module_path!(),
                "::",
                stringify!(#ident),
                "/auto-patch.v1"
            )),
            quote!(concat!(module_path!(), "::", stringify!(#ident))),
        ),
    };

    let crate_path = crate_path.unwrap_or_else(|| syn::parse_quote!(::auto_irokle));

    Ok(Config {
        event_type_id,
        type_label,
        crate_path,
    })
}

fn parse_fields(input: &DeriveInput) -> syn::Result<Vec<AutoField<'_>>> {
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "AutoIrokle can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "AutoIrokle can only be derived for structs",
            ));
        }
    };

    fields
        .iter()
        .map(|field| {
            let ident = field
                .ident
                .as_ref()
                .ok_or_else(|| syn::Error::new_spanned(field, "AutoIrokle fields must be named"))?;
            let kind = resolve_field_kind(field)?;
            Ok(AutoField {
                ident,
                name: ident.to_string(),
                kind,
            })
        })
        .collect()
}

fn resolve_field_kind(field: &Field) -> syn::Result<FieldKind> {
    let mut override_kind: Option<KindOverride> = None;

    for attr in &field.attrs {
        if !attr.path().is_ident("auto_irokle") {
            continue;
        }
        let args = attr.parse_args::<FieldArgs>()?;
        if let Some(kind) = args.kind {
            if override_kind.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "duplicate auto_irokle `kind` attribute on field",
                ));
            }
            override_kind = Some(kind);
        }
    }

    if let Some(kind) = override_kind {
        return kind_from_override(field, kind);
    }

    Ok(detect_field_kind(&field.ty))
}

fn kind_from_override(field: &Field, kind: KindOverride) -> syn::Result<FieldKind> {
    let args = last_segment_args(&field.ty);
    match kind {
        KindOverride::Lww => Ok(FieldKind::Lww),
        KindOverride::Set => match args.and_then(|a| generic_type(a, 0)) {
            Some(item) => Ok(FieldKind::Set { item: Box::new(item) }),
            None => Err(syn::Error::new(
                field.ty.span(),
                "auto_irokle(kind = \"set\") requires the field type to expose its item type as the first generic parameter (e.g. `BTreeSet<T>`)",
            )),
        },
        KindOverride::Map => match args.and_then(|a| Some((generic_type(a, 0)?, generic_type(a, 1)?))) {
            Some((key, value)) => Ok(FieldKind::Map {
                key: Box::new(key),
                value: Box::new(value),
            }),
            None => Err(syn::Error::new(
                field.ty.span(),
                "auto_irokle(kind = \"map\") requires the field type to expose its key/value types as the first two generic parameters (e.g. `BTreeMap<K, V>`)",
            )),
        },
    }
}

fn detect_field_kind(ty: &Type) -> FieldKind {
    let Type::Path(tp) = ty else {
        return FieldKind::Lww;
    };
    let Some(last) = tp.path.segments.last() else {
        return FieldKind::Lww;
    };
    let name = last.ident.to_string();
    let args = &last.arguments;

    match name.as_str() {
        "BTreeSet" | "HashSet" => {
            if !is_canonical_collections_path(&tp.path, &name) {
                return FieldKind::Lww;
            }
            generic_type(args, 0)
                .map(|item| FieldKind::Set { item: Box::new(item) })
                .unwrap_or(FieldKind::Lww)
        }
        "BTreeMap" | "HashMap" => {
            if !is_canonical_collections_path(&tp.path, &name) {
                return FieldKind::Lww;
            }
            match (generic_type(args, 0), generic_type(args, 1)) {
                (Some(key), Some(value)) => FieldKind::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                },
                _ => FieldKind::Lww,
            }
        }
        _ => FieldKind::Lww,
    }
}

fn is_canonical_collections_path(path: &Path, leaf: &str) -> bool {
    let segments: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    match segments.as_slice() {
        [only] => *only == leaf,
        [crate_root, "collections", tail] => {
            (*crate_root == "std" || *crate_root == "alloc") && *tail == leaf
        }
        ["collections", tail] => *tail == leaf,
        _ => false,
    }
}

fn diff_field(crate_path: &Path, field: &AutoField<'_>) -> proc_macro2::TokenStream {
    let ident = field.ident;
    let name = &field.name;
    match &field.kind {
        FieldKind::Lww => quote! {
            if old.#ident != new.#ident {
                ops.push(#crate_path::PatchOp::set(
                    #crate_path::__private::field_path(#name),
                    #crate_path::__private::encode_value(&new.#ident)?,
                ));
            }
        },
        FieldKind::Set { .. } => quote! {
            for value in new.#ident.difference(&old.#ident) {
                ops.push(#crate_path::PatchOp::set_insert(
                    #crate_path::__private::field_path(#name),
                    #crate_path::__private::encode_value(value)?,
                ));
            }
            for value in old.#ident.difference(&new.#ident) {
                ops.push(#crate_path::PatchOp::set_remove(
                    #crate_path::__private::field_path(#name),
                    #crate_path::__private::encode_value(value)?,
                ));
            }
        },
        FieldKind::Map { .. } => quote! {
            for (key, value) in &new.#ident {
                match old.#ident.get(key) {
                    Some(old_value) if old_value == value => {}
                    _ => ops.push(#crate_path::PatchOp::map_set(
                        #crate_path::__private::field_path(#name),
                        #crate_path::__private::encode_value(key)?,
                        #crate_path::__private::encode_value(value)?,
                    )),
                }
            }
            for key in old.#ident.keys() {
                if !new.#ident.contains_key(key) {
                    ops.push(#crate_path::PatchOp::map_remove(
                        #crate_path::__private::field_path(#name),
                        #crate_path::__private::encode_value(key)?,
                    ));
                }
            }
        },
    }
}

fn init_field(crate_path: &Path, field: &AutoField<'_>) -> proc_macro2::TokenStream {
    let ident = field.ident;
    let name = &field.name;
    let values_name = format_ident!("__auto_irokle_{}_values", ident);
    let keys_name = format_ident!("__auto_irokle_{}_keys", ident);
    match &field.kind {
        FieldKind::Lww => quote! {
            projection.init_register(#crate_path::__private::field_path(#name), meta);
        },
        FieldKind::Set { .. } => quote! {
            let #values_name = {
                let state = projection.state()?;
                state
                    .#ident
                    .iter()
                    .map(|value| #crate_path::__private::encode_value(value))
                    .collect::<#crate_path::irokle::Result<Vec<_>>>()?
            };
            projection.init_set_values(#crate_path::__private::field_path(#name), #values_name, meta);
        },
        FieldKind::Map { .. } => quote! {
            let #keys_name = {
                let state = projection.state()?;
                state
                    .#ident
                    .keys()
                    .map(|key| #crate_path::__private::encode_value(key))
                    .collect::<#crate_path::irokle::Result<Vec<_>>>()?
            };
            projection.init_map_keys(#crate_path::__private::field_path(#name), #keys_name, meta);
        },
    }
}

fn apply_field(crate_path: &Path, field: &AutoField<'_>) -> Vec<proc_macro2::TokenStream> {
    let ident = field.ident;
    let name = &field.name;
    match &field.kind {
        FieldKind::Lww => vec![quote! {
            #crate_path::PatchOp::Set { path, value } if #crate_path::__private::path_is(path, &[#name]) => {
                if projection.apply_register(#crate_path::__private::field_path(#name), meta) {
                    projection.state_mut()?.#ident = #crate_path::__private::decode_value(value)?;
                }
                Ok(())
            }
        }],
        FieldKind::Set { item } => vec![
            quote! {
                #crate_path::PatchOp::SetInsert { path, value } if #crate_path::__private::path_is(path, &[#name]) => {
                    if projection.insert_set_value(#crate_path::__private::field_path(#name), value.clone(), meta) {
                        projection.state_mut()?.#ident.insert(#crate_path::__private::decode_value::<#item>(value)?);
                    }
                    Ok(())
                }
            },
            quote! {
                #crate_path::PatchOp::SetRemove { path, value } if #crate_path::__private::path_is(path, &[#name]) => {
                    if !projection.remove_set_value(#crate_path::__private::field_path(#name), value, meta) {
                        let value = #crate_path::__private::decode_value::<#item>(value)?;
                        projection.state_mut()?.#ident.remove(&value);
                    }
                    Ok(())
                }
            },
        ],
        FieldKind::Map {
            key,
            value: map_value,
        } => vec![
            quote! {
                #crate_path::PatchOp::MapSet { path, key, value } if #crate_path::__private::path_is(path, &[#name]) => {
                    if projection.set_map_value(#crate_path::__private::field_path(#name), key.clone(), meta) {
                        projection
                            .state_mut()?
                            .#ident
                            .insert(#crate_path::__private::decode_value::<#key>(key)?, #crate_path::__private::decode_value::<#map_value>(value)?);
                    }
                    Ok(())
                }
            },
            quote! {
                #crate_path::PatchOp::MapRemove { path, key } if #crate_path::__private::path_is(path, &[#name]) => {
                    if !projection.remove_map_key(#crate_path::__private::field_path(#name), key, meta) {
                        let key = #crate_path::__private::decode_value::<#key>(key)?;
                        projection.state_mut()?.#ident.remove(&key);
                    }
                    Ok(())
                }
            },
        ],
    }
}

fn last_segment_args(ty: &Type) -> Option<&PathArguments> {
    let Type::Path(path) = ty else {
        return None;
    };
    path.path.segments.last().map(|segment| &segment.arguments)
}

fn generic_type(arguments: &PathArguments, index: usize) -> Option<Type> {
    let PathArguments::AngleBracketed(arguments) = arguments else {
        return None;
    };
    arguments
        .args
        .iter()
        .filter_map(|argument| match argument {
            GenericArgument::Type(ty) => Some(ty.clone()),
            _ => None,
        })
        .nth(index)
}

fn validate_type_id(type_id: &LitStr) -> syn::Result<()> {
    let value = type_id.value();

    if value.is_empty() {
        return Err(syn::Error::new(
            type_id.span(),
            "auto_irokle `type_id` must not be empty",
        ));
    }

    if value
        .chars()
        .any(|ch| ch.is_control() || ch.is_whitespace())
    {
        return Err(syn::Error::new(
            type_id.span(),
            "auto_irokle `type_id` must not contain whitespace or control characters",
        ));
    }

    Ok(())
}
