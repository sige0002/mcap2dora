//! Parse ros2msg / ros1msg schema text (as embedded in MCAP Schema records)
//! into a resolved type tree, and map it to an Arrow schema.

use anyhow::{bail, Result};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Prim {
    Bool,
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Str,
    WStr,
}

#[derive(Clone, Debug)]
pub enum ArrayKind {
    Fixed(usize),
    /// unbounded and bounded sequences are both variable-length lists
    Unbounded,
}

#[derive(Clone, Debug)]
pub enum FieldType {
    Prim(Prim),
    /// index into TypeReg::defs
    Complex(usize),
    Array(Box<FieldType>, ArrayKind),
}

#[derive(Debug)]
pub struct MsgDef {
    pub fields: Vec<(String, FieldType)>,
}

pub struct TypeReg {
    pub defs: Vec<MsgDef>,
    pub top: usize,
}

struct RawDef {
    name: String, // normalized: "pkg/Type"
    pkg: String,
    fields: Vec<(String, String)>, // (type token, field name)
}

fn norm_name(n: &str) -> String {
    n.trim().replace("/msg/", "/")
}

fn parse_sections(schema_name: &str, text: &str) -> Result<Vec<RawDef>> {
    let mut sections: Vec<Vec<&str>> = vec![Vec::new()];
    for line in text.lines() {
        if line.starts_with("===") {
            sections.push(Vec::new());
        } else {
            sections.last_mut().unwrap().push(line);
        }
    }
    let mut raws = Vec::new();
    for (i, sec) in sections.iter().enumerate() {
        let mut name = if i == 0 {
            norm_name(schema_name)
        } else {
            String::new()
        };
        let mut fields = Vec::new();
        for line in sec {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("MSG:") {
                if i > 0 && name.is_empty() {
                    name = norm_name(rest);
                }
                continue;
            }
            let line = match line.find('#') {
                Some(p) => line[..p].trim(),
                None => line,
            };
            if line.is_empty() {
                continue;
            }
            let toks: Vec<&str> = line.split_whitespace().collect();
            if toks.len() < 2 {
                continue;
            }
            // constants: "uint8 FOO=1" or "uint8 FOO = 1"
            if toks[1].contains('=') || (toks.len() >= 3 && toks[2] == "=") {
                continue;
            }
            fields.push((toks[0].to_string(), toks[1].to_string()));
        }
        if name.is_empty() {
            continue; // separator with no MSG line / empty trailing section
        }
        let pkg = name.split('/').next().unwrap_or("").to_string();
        raws.push(RawDef { name, pkg, fields });
    }
    if raws.is_empty() {
        bail!("empty schema");
    }
    Ok(raws)
}

/// Inject builtin definitions used when the concatenated schema text omits them.
fn inject_builtins(raws: &mut Vec<RawDef>, ros1: bool) {
    let have: Vec<String> = raws.iter().map(|r| r.name.clone()).collect();
    let mut add = |name: &str, fields: &[(&str, &str)]| {
        if !have.iter().any(|n| n == name) {
            raws.push(RawDef {
                name: name.to_string(),
                pkg: name.split('/').next().unwrap_or("").to_string(),
                fields: fields
                    .iter()
                    .map(|(t, n)| (t.to_string(), n.to_string()))
                    .collect(),
            });
        }
    };
    if ros1 {
        add("__ros1/time", &[("uint32", "secs"), ("uint32", "nsecs")]);
        add("__ros1/duration", &[("int32", "secs"), ("int32", "nsecs")]);
    } else {
        add(
            "builtin_interfaces/Time",
            &[("int32", "sec"), ("uint32", "nanosec")],
        );
        add(
            "builtin_interfaces/Duration",
            &[("int32", "sec"), ("uint32", "nanosec")],
        );
    }
}

fn prim_of(tok: &str, ros1: bool) -> Option<Prim> {
    Some(match tok {
        "bool" => Prim::Bool,
        "int8" => Prim::I8,
        "uint8" => Prim::U8,
        "byte" => {
            if ros1 {
                Prim::I8
            } else {
                Prim::U8
            }
        }
        "char" => Prim::U8,
        "int16" => Prim::I16,
        "uint16" => Prim::U16,
        "int32" => Prim::I32,
        "uint32" => Prim::U32,
        "int64" => Prim::I64,
        "uint64" => Prim::U64,
        "float32" => Prim::F32,
        "float64" => Prim::F64,
        "string" => Prim::Str,
        "wstring" => Prim::WStr,
        _ => return None,
    })
}

struct Resolver<'a> {
    raws: &'a [RawDef],
    by_name: HashMap<&'a str, usize>,
    defs: Vec<Option<MsgDef>>,
    ros1: bool,
}

impl<'a> Resolver<'a> {
    fn resolve_def(&mut self, idx: usize, depth: usize) -> Result<()> {
        if self.defs[idx].is_some() {
            return Ok(());
        }
        if depth > 64 {
            bail!("type recursion too deep");
        }
        // placeholder to allow index reservation; ROS msg types cannot be cyclic,
        // recursion is resolved depth-first before use
        let raw = &self.raws[idx];
        let pkg = raw.pkg.clone();
        let fields_src = raw.fields.clone();
        let mut fields = Vec::with_capacity(fields_src.len());
        for (tok, name) in fields_src {
            let ft = self.resolve_type(&tok, &pkg, depth)?;
            fields.push((name, ft));
        }
        self.defs[idx] = Some(MsgDef { fields });
        Ok(())
    }

    fn resolve_type(&mut self, tok: &str, pkg: &str, depth: usize) -> Result<FieldType> {
        // array suffix
        if let Some(p) = tok.find('[') {
            if !tok.ends_with(']') {
                bail!("bad array type: {tok}");
            }
            let inner_tok = &tok[..p];
            let dim = &tok[p + 1..tok.len() - 1];
            let kind = if dim.is_empty() || dim.starts_with("<=") {
                ArrayKind::Unbounded
            } else {
                ArrayKind::Fixed(dim.parse()?)
            };
            let inner = self.resolve_type(inner_tok, pkg, depth)?;
            return Ok(FieldType::Array(Box::new(inner), kind));
        }
        // bounded string: string<=N / wstring<=N
        let base = match tok.find("<=") {
            Some(p) => &tok[..p],
            None => tok,
        };
        if let Some(pr) = prim_of(base, self.ros1) {
            return Ok(FieldType::Prim(pr));
        }
        if self.ros1 {
            if base == "time" {
                return self.complex_by_name("__ros1/time", depth);
            }
            if base == "duration" {
                return self.complex_by_name("__ros1/duration", depth);
            }
        }
        // complex type reference
        let cand = norm_name(base);
        if cand.contains('/') {
            return self.complex_by_name(&cand, depth);
        }
        // bare name: same package, then Header special case, then suffix match
        let same_pkg = format!("{pkg}/{cand}");
        if self.by_name.contains_key(same_pkg.as_str()) {
            return self.complex_by_name(&same_pkg, depth);
        }
        if cand == "Header" {
            return self.complex_by_name("std_msgs/Header", depth);
        }
        let suffix = format!("/{cand}");
        let found: Vec<usize> = self
            .by_name
            .iter()
            .filter(|(n, _)| n.ends_with(suffix.as_str()))
            .map(|(_, i)| *i)
            .collect();
        match found.as_slice() {
            [i] => {
                let i = *i;
                self.resolve_def(i, depth + 1)?;
                Ok(FieldType::Complex(i))
            }
            [] => bail!("unresolved type reference: {tok}"),
            _ => bail!("ambiguous type reference: {tok}"),
        }
    }

    fn complex_by_name(&mut self, name: &str, depth: usize) -> Result<FieldType> {
        match self.by_name.get(name).copied() {
            Some(i) => {
                self.resolve_def(i, depth + 1)?;
                Ok(FieldType::Complex(i))
            }
            None => bail!("unresolved type reference: {name}"),
        }
    }
}

pub fn parse(schema_name: &str, text: &str, ros1: bool) -> Result<TypeReg> {
    let mut raws = parse_sections(schema_name, text)?;
    inject_builtins(&mut raws, ros1);
    let by_name: HashMap<&str, usize> = raws
        .iter()
        .enumerate()
        .map(|(i, r)| (r.name.as_str(), i))
        .collect();
    let n = raws.len();
    let mut res = Resolver {
        raws: &raws,
        by_name,
        defs: (0..n).map(|_| None).collect(),
        ros1,
    };
    for i in 0..n {
        res.resolve_def(i, 0)?;
    }
    let defs: Vec<MsgDef> = res.defs.into_iter().map(|d| d.unwrap()).collect();
    Ok(TypeReg { defs, top: 0 })
}

pub fn prim_dt(p: Prim) -> DataType {
    match p {
        Prim::Bool => DataType::Boolean,
        Prim::I8 => DataType::Int8,
        Prim::U8 => DataType::UInt8,
        Prim::I16 => DataType::Int16,
        Prim::U16 => DataType::UInt16,
        Prim::I32 => DataType::Int32,
        Prim::U32 => DataType::UInt32,
        Prim::I64 => DataType::Int64,
        Prim::U64 => DataType::UInt64,
        Prim::F32 => DataType::Float32,
        Prim::F64 => DataType::Float64,
        Prim::Str | Prim::WStr => DataType::Utf8,
    }
}

pub fn arrow_type(ft: &FieldType, defs: &[MsgDef]) -> DataType {
    match ft {
        FieldType::Prim(p) => prim_dt(*p),
        FieldType::Complex(i) => DataType::Struct(struct_fields(*i, defs)),
        FieldType::Array(inner, kind) => {
            if matches!(inner.as_ref(), FieldType::Prim(Prim::U8)) {
                return match kind {
                    ArrayKind::Fixed(n) => DataType::FixedSizeBinary(*n as i32),
                    ArrayKind::Unbounded => DataType::LargeBinary,
                };
            }
            let item = Arc::new(Field::new("item", arrow_type(inner, defs), false));
            match kind {
                ArrayKind::Fixed(n) => DataType::FixedSizeList(item, *n as i32),
                ArrayKind::Unbounded => DataType::List(item),
            }
        }
    }
}

pub fn struct_fields(def_idx: usize, defs: &[MsgDef]) -> Fields {
    defs[def_idx]
        .fields
        .iter()
        .map(|(name, ft)| Field::new(name.clone(), arrow_type(ft, defs), false))
        .collect()
}

pub fn timestamp_dt() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}
