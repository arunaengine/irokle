use proc_macro::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::parse_quote;
use syn::{DeriveInput, LitStr, Path, Token, parse_macro_input};

#[proc_macro_derive(Event, attributes(irokle))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand_event(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

struct IrokleArgs {
    entries: Vec<IrokleArg>,
}

enum IrokleArg {
    TypeId(LitStr),
    Crate(LitStr),
}

impl Parse for IrokleArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut entries = Vec::new();

        while !input.is_empty() {
            let key = syn::Ident::parse_any(input)?;
            input.parse::<Token![=]>()?;

            if key == "type_id" {
                let value: LitStr = input.parse()?;
                entries.push(IrokleArg::TypeId(value));
            } else if key == "crate" {
                let value: LitStr = input.parse()?;
                entries.push(IrokleArg::Crate(value));
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unsupported irokle attribute; expected `type_id` or `crate`",
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

struct EventConfig {
    type_id: proc_macro2::TokenStream,
    crate_path: Path,
}

fn expand_event(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let config = parse_config(input)?;
    let ident = &input.ident;
    let mut generics = input.generics.clone();
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(Self: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync + 'static));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let type_id = &config.type_id;
    let crate_path = &config.crate_path;

    Ok(quote! {
        impl #impl_generics #crate_path::Event for #ident #type_generics #where_clause
        {
            const TYPE_ID: &'static str = #type_id;
        }
    })
}

fn parse_config(input: &DeriveInput) -> syn::Result<EventConfig> {
    let mut type_id = None;
    let mut crate_path = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("irokle") {
            continue;
        }

        let args = attr.parse_args::<IrokleArgs>()?;

        for entry in args.entries {
            match entry {
                IrokleArg::TypeId(value) => {
                    if type_id.is_some() {
                        return Err(syn::Error::new(
                            value.span(),
                            "duplicate irokle `type_id` attribute",
                        ));
                    }

                    validate_type_id(&value)?;
                    type_id = Some(value);
                }
                IrokleArg::Crate(value) => {
                    if crate_path.is_some() {
                        return Err(syn::Error::new(
                            value.span(),
                            "duplicate irokle `crate` attribute",
                        ));
                    }

                    let path = value.parse::<Path>().map_err(|_| {
                        syn::Error::new(value.span(), "irokle `crate` must be a Rust path")
                    })?;
                    crate_path = Some(path);
                }
            }
        }
    }

    let ident = &input.ident;
    let type_id = type_id
        .map(|type_id| quote!(#type_id))
        .unwrap_or_else(|| quote!(concat!(module_path!(), "::", stringify!(#ident))));

    let crate_path = match crate_path {
        Some(path) => path,
        None => syn::parse_quote!(::irokle),
    };

    Ok(EventConfig {
        type_id,
        crate_path,
    })
}

fn validate_type_id(type_id: &LitStr) -> syn::Result<()> {
    let value = type_id.value();

    if value.is_empty() {
        return Err(syn::Error::new(
            type_id.span(),
            "irokle `type_id` must not be empty",
        ));
    }

    if value
        .chars()
        .any(|ch| ch.is_control() || ch.is_whitespace())
    {
        return Err(syn::Error::new(
            type_id.span(),
            "irokle `type_id` must not contain whitespace or control characters",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(tokens: proc_macro2::TokenStream) -> DeriveInput {
        syn::parse2(tokens).unwrap()
    }

    fn parse_config_err(input: &DeriveInput) -> syn::Error {
        match parse_config(input) {
            Ok(_) => panic!("expected parse_config to fail"),
            Err(err) => err,
        }
    }

    #[test]
    fn parses_type_id() {
        let input = input(quote! {
            #[irokle(type_id = "example.chat")]
            struct Chat;
        });

        let config = parse_config(&input).unwrap();

        assert_eq!(config.type_id.to_string(), "\"example.chat\"");
    }

    #[test]
    fn infers_missing_type_id() {
        let input = input(quote! { struct Chat; });
        let config = parse_config(&input).unwrap();

        assert_eq!(
            config.type_id.to_string(),
            "concat ! (module_path ! () , \"::\" , stringify ! (Chat))"
        );
    }

    #[test]
    fn rejects_empty_type_id() {
        let input = input(quote! {
            #[irokle(type_id = "")]
            struct Chat;
        });
        let err = parse_config_err(&input);

        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn rejects_duplicate_type_id() {
        let input = input(quote! {
            #[irokle(type_id = "one", type_id = "two")]
            struct Chat;
        });
        let err = parse_config_err(&input);

        assert!(err.to_string().contains("duplicate irokle `type_id`"));
    }

    #[test]
    fn rejects_unknown_key() {
        let input = input(quote! {
            #[irokle(type_id = "example.chat", schema_version = 1)]
            struct Chat;
        });
        let err = parse_config_err(&input);

        assert!(err.to_string().contains("unsupported irokle attribute"));
    }
}
