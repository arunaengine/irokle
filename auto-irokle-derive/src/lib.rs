// SPDX-License-Identifier: MIT OR Apache-2.0
//! Proc-macro implementation for deriving `auto_irokle::AutoIrokle`.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::parse_quote;
use syn::{
    Data, DeriveInput, Fields, GenericArgument, LitStr, Path, PathArguments, Token, Type,
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

struct AutoIrokleArgs {
    entries: Vec<AutoIrokleArg>,
}

enum AutoIrokleArg {
    TypeId(LitStr),
    Crate(LitStr),
}

impl Parse for AutoIrokleArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut entries = Vec::new();

        while !input.is_empty() {
            let key = syn::Ident::parse_any(input)?;
            input.parse::<Token![=]>()?;

            if key == "type_id" {
                entries.push(AutoIrokleArg::TypeId(input.parse()?));
            } else if key == "crate" {
                entries.push(AutoIrokleArg::Crate(input.parse()?));
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unsupported auto_irokle attribute; expected `type_id` or `crate`",
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

struct Config {
    type_id: proc_macro2::TokenStream,
    event_type_id: proc_macro2::TokenStream,
    crate_path: Path,
}

enum FieldKind {
    Lww,
    Set { item: Type },
    Map { key: Type, value: Type },
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
            + #crate_path::facet::Facet<'static>
            + Send
            + Sync
            + 'static
    ));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let type_id = &config.type_id;
    let event_type_id = &config.event_type_id;

    Ok(quote! {
        impl #impl_generics #crate_path::AutoIrokle for #ident #type_generics #where_clause {
            const TYPE_ID: &'static str = #type_id;
            const EVENT_TYPE_ID: &'static str = #event_type_id;

            fn diff(old: &Self, new: &Self) -> #crate_path::irokle::Result<Vec<#crate_path::PatchOp>> {
                let mut ops = Vec::new();
                #(#diff_fields)*
                Ok(ops)
            }

            fn apply_init(
                projection: &mut #crate_path::AutoProjection<Self>,
                value: Self,
                meta: &#crate_path::irokle::reducer::OpMeta,
            ) -> #crate_path::irokle::Result<()> {
                projection.replace_state(value);
                #(#init_fields)*
                Ok(())
            }

            fn apply_patch_op(
                projection: &mut #crate_path::AutoProjection<Self>,
                op: &#crate_path::PatchOp,
                meta: &#crate_path::irokle::reducer::OpMeta,
            ) -> #crate_path::irokle::Result<()> {
                match op {
                    #(#apply_arms)*
                    _ => Err(#crate_path::irokle::Error::Decode(format!(
                        "unsupported auto-irokle patch op for {}",
                        <Self as #crate_path::AutoIrokle>::TYPE_ID,
                    ))),
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

        let args = attr.parse_args::<AutoIrokleArgs>()?;
        for entry in args.entries {
            match entry {
                AutoIrokleArg::TypeId(value) => {
                    if type_id.is_some() {
                        return Err(syn::Error::new(
                            value.span(),
                            "duplicate auto_irokle `type_id` attribute",
                        ));
                    }
                    validate_type_id(&value)?;
                    type_id = Some(value);
                }
                AutoIrokleArg::Crate(value) => {
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
    let (type_id, event_type_id) = match type_id {
        Some(type_id) => {
            let event_type_id = LitStr::new(
                &format!("{}/auto-patch.v1", type_id.value()),
                type_id.span(),
            );
            (quote!(#type_id), quote!(#event_type_id))
        }
        None => (
            quote!(concat!(module_path!(), "::", stringify!(#ident))),
            quote!(concat!(
                module_path!(),
                "::",
                stringify!(#ident),
                "/auto-patch.v1"
            )),
        ),
    };

    let crate_path = crate_path.unwrap_or_else(|| syn::parse_quote!(::auto_irokle));

    Ok(Config {
        type_id,
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
            Ok(AutoField {
                ident,
                name: ident.to_string(),
                kind: field_kind(&field.ty),
            })
        })
        .collect()
}

fn diff_field(crate_path: &Path, field: &AutoField<'_>) -> proc_macro2::TokenStream {
    let ident = field.ident;
    let name = &field.name;
    match &field.kind {
        FieldKind::Lww => quote! {
            if old.#ident != new.#ident {
                ops.push(#crate_path::PatchOp::set(
                    #crate_path::field_path(#name),
                    #crate_path::encode_value(&new.#ident)?,
                ));
            }
        },
        FieldKind::Set { .. } => quote! {
            for value in new.#ident.difference(&old.#ident) {
                ops.push(#crate_path::PatchOp::set_insert(
                    #crate_path::field_path(#name),
                    #crate_path::encode_value(value)?,
                ));
            }
            for value in old.#ident.difference(&new.#ident) {
                ops.push(#crate_path::PatchOp::set_remove(
                    #crate_path::field_path(#name),
                    #crate_path::encode_value(value)?,
                ));
            }
        },
        FieldKind::Map { .. } => quote! {
            for (key, value) in &new.#ident {
                match old.#ident.get(key) {
                    Some(old_value) if old_value == value => {}
                    _ => ops.push(#crate_path::PatchOp::map_set(
                        #crate_path::field_path(#name),
                        #crate_path::encode_value(key)?,
                        #crate_path::encode_value(value)?,
                    )),
                }
            }
            for key in old.#ident.keys() {
                if !new.#ident.contains_key(key) {
                    ops.push(#crate_path::PatchOp::map_remove(
                        #crate_path::field_path(#name),
                        #crate_path::encode_value(key)?,
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
            projection.init_register(#crate_path::field_path(#name), meta);
        },
        FieldKind::Set { .. } => quote! {
            let #values_name = {
                let state = projection.state()?;
                state
                    .#ident
                    .iter()
                    .map(|value| #crate_path::encode_value(value))
                    .collect::<#crate_path::irokle::Result<Vec<_>>>()?
            };
            projection.init_set_values(#crate_path::field_path(#name), #values_name, meta);
        },
        FieldKind::Map { .. } => quote! {
            let #keys_name = {
                let state = projection.state()?;
                state
                    .#ident
                    .keys()
                    .map(|key| #crate_path::encode_value(key))
                    .collect::<#crate_path::irokle::Result<Vec<_>>>()?
            };
            projection.init_map_keys(#crate_path::field_path(#name), #keys_name, meta);
        },
    }
}

fn apply_field(crate_path: &Path, field: &AutoField<'_>) -> Vec<proc_macro2::TokenStream> {
    let ident = field.ident;
    let name = &field.name;
    match &field.kind {
        FieldKind::Lww => vec![quote! {
            #crate_path::PatchOp::Set { path, value } if #crate_path::path_is(path, &[#name]) => {
                if projection.apply_register(#crate_path::field_path(#name), meta) {
                    projection.state_mut()?.#ident = #crate_path::decode_value(value)?;
                }
                Ok(())
            }
        }],
        FieldKind::Set { item } => vec![
            quote! {
                #crate_path::PatchOp::SetInsert { path, value } if #crate_path::path_is(path, &[#name]) => {
                    if projection.insert_set_value(#crate_path::field_path(#name), value.clone(), meta) {
                        projection.state_mut()?.#ident.insert(#crate_path::decode_value::<#item>(value)?);
                    }
                    Ok(())
                }
            },
            quote! {
                #crate_path::PatchOp::SetRemove { path, value } if #crate_path::path_is(path, &[#name]) => {
                    if !projection.remove_set_value(#crate_path::field_path(#name), value, meta) {
                        let value = #crate_path::decode_value::<#item>(value)?;
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
                #crate_path::PatchOp::MapSet { path, key, value } if #crate_path::path_is(path, &[#name]) => {
                    if projection.set_map_value(#crate_path::field_path(#name), key.clone(), meta) {
                        projection
                            .state_mut()?
                            .#ident
                            .insert(#crate_path::decode_value::<#key>(key)?, #crate_path::decode_value::<#map_value>(value)?);
                    }
                    Ok(())
                }
            },
            quote! {
                #crate_path::PatchOp::MapRemove { path, key } if #crate_path::path_is(path, &[#name]) => {
                    if !projection.remove_map_key(#crate_path::field_path(#name), key, meta) {
                        let key = #crate_path::decode_value::<#key>(key)?;
                        projection.state_mut()?.#ident.remove(&key);
                    }
                    Ok(())
                }
            },
        ],
    }
}

fn field_kind(ty: &Type) -> FieldKind {
    let Some((name, arguments)) = type_segment(ty) else {
        return FieldKind::Lww;
    };

    match name.as_str() {
        "BTreeSet" | "HashSet" => generic_type(arguments, 0)
            .map(|item| FieldKind::Set { item })
            .unwrap_or(FieldKind::Lww),
        "BTreeMap" | "HashMap" => match (generic_type(arguments, 0), generic_type(arguments, 1)) {
            (Some(key), Some(value)) => FieldKind::Map { key, value },
            _ => FieldKind::Lww,
        },
        _ => FieldKind::Lww,
    }
}

fn type_segment(ty: &Type) -> Option<(String, &PathArguments)> {
    let Type::Path(path) = ty else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| (segment.ident.to_string(), &segment.arguments))
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
