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

    match expand(&input) {
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
    Nested,
}

impl Parse for StructArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut entries = Vec::new();
        while !input.is_empty() {
            let key = syn::Ident::parse_any(input)?;
            if key == "nested" {
                entries.push(StructArg::Nested);
            } else {
                input.parse::<Token![=]>()?;
                if key == "type_id" {
                    entries.push(StructArg::TypeId(input.parse()?));
                } else if key == "crate" {
                    entries.push(StructArg::Crate(input.parse()?));
                } else {
                    return Err(syn::Error::new(
                        key.span(),
                        "unsupported struct-level auto_irokle attribute; expected `type_id`, `crate`, or `nested`",
                    ));
                }
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
    Nested,
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
                    "nested" => KindOverride::Nested,
                    other => {
                        return Err(syn::Error::new(
                            value.span(),
                            format!(
                                "auto_irokle `kind` must be one of \"lww\", \"set\", \"map\", or \"nested\"; got {other:?}"
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
    is_root: bool,
    event_type_id: proc_macro2::TokenStream,
    crate_path: Path,
}

enum FieldKind {
    Lww,
    Set { item: Box<Type> },
    Map { key: Box<Type>, value: Box<Type> },
    Nested { ty: Box<Type> },
}

struct AutoField<'a> {
    ident: &'a syn::Ident,
    name: String,
    kind: FieldKind,
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let config = parse_config(input)?;
    let ident = &input.ident;
    let fields = parse_fields(input)?;
    let crate_path = &config.crate_path;

    let diff_arms = fields.iter().map(|f| diff_field(crate_path, f));
    let init_arms = fields.iter().map(|f| init_field(crate_path, f));
    let apply_arms = fields.iter().map(|f| apply_field(crate_path, f));

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

    let auto_crdt_impl = quote! {
        impl #impl_generics #crate_path::AutoCrdt for #ident #type_generics #where_clause {
            fn diff_into(
                prefix: &[String],
                old: &Self,
                new: &Self,
                ops: &mut Vec<#crate_path::PatchOp>,
            ) -> #crate_path::irokle::Result<()> {
                #(#diff_arms)*
                Ok(())
            }

            fn init_into(
                prefix: &[String],
                state: &Self,
                meta: &mut #crate_path::ProjectionMeta,
                op_meta: &#crate_path::__private::OpMeta,
            ) -> #crate_path::irokle::Result<()> {
                #(#init_arms)*
                Ok(())
            }

            fn apply_into(
                prefix: &[String],
                state: &mut Self,
                meta: &mut #crate_path::ProjectionMeta,
                op: &#crate_path::PatchOp,
                op_meta: &#crate_path::__private::OpMeta,
            ) -> #crate_path::irokle::Result<bool> {
                #(#apply_arms)*
                Ok(false)
            }
        }
    };

    let auto_irokle_impl = if config.is_root {
        let event_type_id = &config.event_type_id;
        quote! {
            impl #impl_generics #crate_path::AutoIrokle for #ident #type_generics #where_clause {
                const EVENT_TYPE_ID: &'static str = #event_type_id;
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #auto_crdt_impl
        #auto_irokle_impl
    })
}

fn parse_config(input: &DeriveInput) -> syn::Result<Config> {
    let mut type_id: Option<LitStr> = None;
    let mut crate_path: Option<Path> = None;
    let mut nested = false;

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
                StructArg::Nested => {
                    if nested {
                        return Err(syn::Error::new(
                            input.ident.span(),
                            "duplicate auto_irokle `nested` attribute",
                        ));
                    }
                    nested = true;
                }
            }
        }
    }

    if nested && type_id.is_some() {
        return Err(syn::Error::new(
            input.ident.span(),
            "auto_irokle struct cannot be both `nested` and have a `type_id`",
        ));
    }

    let ident = &input.ident;
    let event_type_id = match type_id {
        Some(type_id) => {
            let event_type_id = LitStr::new(
                &format!("{}/auto-patch.v1", type_id.value()),
                type_id.span(),
            );
            quote!(#event_type_id)
        }
        None => quote!(concat!(
            module_path!(),
            "::",
            stringify!(#ident),
            "/auto-patch.v1"
        )),
    };

    let crate_path = crate_path.unwrap_or_else(|| syn::parse_quote!(::auto_irokle));

    Ok(Config {
        is_root: !nested,
        event_type_id,
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
            Some(item) => Ok(FieldKind::Set {
                item: Box::new(item),
            }),
            None => Err(syn::Error::new(
                field.ty.span(),
                "auto_irokle(kind = \"set\") requires the field type to expose its item type as the first generic parameter (e.g. `BTreeSet<T>`)",
            )),
        },
        KindOverride::Map => {
            match args.and_then(|a| Some((generic_type(a, 0)?, generic_type(a, 1)?))) {
                Some((key, value)) => Ok(FieldKind::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                }),
                None => Err(syn::Error::new(
                    field.ty.span(),
                    "auto_irokle(kind = \"map\") requires the field type to expose its key/value types as the first two generic parameters (e.g. `BTreeMap<K, V>`)",
                )),
            }
        }
        KindOverride::Nested => Ok(FieldKind::Nested {
            ty: Box::new(field.ty.clone()),
        }),
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
                .map(|item| FieldKind::Set {
                    item: Box::new(item),
                })
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
                    #crate_path::__private::extend_prefix(prefix, #name),
                    #crate_path::__private::encode_value(&new.#ident)?,
                ));
            }
        },
        FieldKind::Set { .. } => quote! {
            {
                let here = #crate_path::__private::extend_prefix(prefix, #name);
                for value in new.#ident.difference(&old.#ident) {
                    ops.push(#crate_path::PatchOp::set_insert(
                        here.clone(),
                        #crate_path::__private::encode_value(value)?,
                    ));
                }
                for value in old.#ident.difference(&new.#ident) {
                    ops.push(#crate_path::PatchOp::set_remove(
                        here.clone(),
                        #crate_path::__private::encode_value(value)?,
                    ));
                }
            }
        },
        FieldKind::Map { .. } => quote! {
            {
                let here = #crate_path::__private::extend_prefix(prefix, #name);
                for (key, value) in &new.#ident {
                    match old.#ident.get(key) {
                        Some(old_value) if old_value == value => {}
                        _ => ops.push(#crate_path::PatchOp::map_set(
                            here.clone(),
                            #crate_path::__private::encode_value(key)?,
                            #crate_path::__private::encode_value(value)?,
                        )),
                    }
                }
                for key in old.#ident.keys() {
                    if !new.#ident.contains_key(key) {
                        ops.push(#crate_path::PatchOp::map_remove(
                            here.clone(),
                            #crate_path::__private::encode_value(key)?,
                        ));
                    }
                }
            }
        },
        FieldKind::Nested { ty } => quote! {
            {
                let here = #crate_path::__private::extend_prefix(prefix, #name);
                <#ty as #crate_path::AutoCrdt>::diff_into(&here, &old.#ident, &new.#ident, ops)?;
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
            meta.init_register(#crate_path::__private::extend_prefix(prefix, #name), op_meta);
        },
        FieldKind::Set { .. } => quote! {
            {
                let here = #crate_path::__private::extend_prefix(prefix, #name);
                let #values_name = state
                    .#ident
                    .iter()
                    .map(|value| #crate_path::__private::encode_value(value))
                    .collect::<#crate_path::irokle::Result<Vec<_>>>()?;
                meta.init_set_values(here, #values_name, op_meta);
            }
        },
        FieldKind::Map { .. } => quote! {
            {
                let here = #crate_path::__private::extend_prefix(prefix, #name);
                let #keys_name = state
                    .#ident
                    .keys()
                    .map(|key| #crate_path::__private::encode_value(key))
                    .collect::<#crate_path::irokle::Result<Vec<_>>>()?;
                meta.init_map_keys(here, #keys_name, op_meta);
            }
        },
        FieldKind::Nested { ty } => quote! {
            {
                let here = #crate_path::__private::extend_prefix(prefix, #name);
                <#ty as #crate_path::AutoCrdt>::init_into(&here, &state.#ident, meta, op_meta)?;
            }
        },
    }
}

fn apply_field(crate_path: &Path, field: &AutoField<'_>) -> proc_macro2::TokenStream {
    let ident = field.ident;
    let name = &field.name;
    match &field.kind {
        FieldKind::Lww => quote! {
            if let #crate_path::PatchOp::Set { path, value } = op {
                if #crate_path::__private::path_matches(path, prefix, #name) {
                    let here = #crate_path::__private::extend_prefix(prefix, #name);
                    if meta.apply_register(here, op_meta) {
                        state.#ident = #crate_path::__private::decode_value(value)?;
                    }
                    return Ok(true);
                }
            }
        },
        FieldKind::Set { item } => quote! {
            match op {
                #crate_path::PatchOp::SetInsert { path, value }
                    if #crate_path::__private::path_matches(path, prefix, #name) =>
                {
                    let here = #crate_path::__private::extend_prefix(prefix, #name);
                    if meta.insert_set_value(here, value.clone(), op_meta) {
                        state.#ident.insert(
                            #crate_path::__private::decode_value::<#item>(value)?,
                        );
                    }
                    return Ok(true);
                }
                #crate_path::PatchOp::SetRemove { path, value }
                    if #crate_path::__private::path_matches(path, prefix, #name) =>
                {
                    let here = #crate_path::__private::extend_prefix(prefix, #name);
                    if !meta.remove_set_value(here, value, op_meta) {
                        let value = #crate_path::__private::decode_value::<#item>(value)?;
                        state.#ident.remove(&value);
                    }
                    return Ok(true);
                }
                _ => {}
            }
        },
        FieldKind::Map {
            key,
            value: map_value,
        } => quote! {
            match op {
                #crate_path::PatchOp::MapSet { path, key, value }
                    if #crate_path::__private::path_matches(path, prefix, #name) =>
                {
                    let here = #crate_path::__private::extend_prefix(prefix, #name);
                    if meta.set_map_value(here, key.clone(), op_meta) {
                        state.#ident.insert(
                            #crate_path::__private::decode_value::<#key>(key)?,
                            #crate_path::__private::decode_value::<#map_value>(value)?,
                        );
                    }
                    return Ok(true);
                }
                #crate_path::PatchOp::MapRemove { path, key }
                    if #crate_path::__private::path_matches(path, prefix, #name) =>
                {
                    let here = #crate_path::__private::extend_prefix(prefix, #name);
                    if !meta.remove_map_key(here, key, op_meta) {
                        let key = #crate_path::__private::decode_value::<#key>(key)?;
                        state.#ident.remove(&key);
                    }
                    return Ok(true);
                }
                _ => {}
            }
        },
        FieldKind::Nested { ty } => quote! {
            {
                let here = #crate_path::__private::extend_prefix(prefix, #name);
                if <#ty as #crate_path::AutoCrdt>::apply_into(
                    &here,
                    &mut state.#ident,
                    meta,
                    op,
                    op_meta,
                )? {
                    return Ok(true);
                }
            }
        },
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
