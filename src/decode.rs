//! CDR (ROS 2) / ROS 1 binary deserialization directly into Arrow column builders.

use crate::rosmsg::{ArrayKind, FieldType, MsgDef, Prim};
use anyhow::{bail, Result};
use arrow::array::*;
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;
use std::sync::Arc;

pub struct Reader<'a> {
    d: &'a [u8],
    pos: usize,
    le: bool,
    aligned: bool, // CDR aligns primitives; ROS1 serialization is packed
}

macro_rules! read_prim {
    ($fn:ident, $ty:ty, $n:expr) => {
        #[inline]
        pub fn $fn(&mut self) -> Result<$ty> {
            self.align($n);
            let b = self.take($n)?;
            let arr: [u8; $n] = b.try_into().unwrap();
            Ok(if self.le {
                <$ty>::from_le_bytes(arr)
            } else {
                <$ty>::from_be_bytes(arr)
            })
        }
    };
}

impl<'a> Reader<'a> {
    pub fn cdr(payload: &'a [u8]) -> Result<Self> {
        if payload.len() < 4 {
            bail!("payload too short for CDR encapsulation");
        }
        let le = match payload[1] {
            0 => false,
            1 => true,
            other => bail!("unsupported CDR encapsulation kind {other}"),
        };
        Ok(Reader {
            d: &payload[4..],
            pos: 0,
            le,
            aligned: true,
        })
    }

    pub fn ros1(payload: &'a [u8]) -> Self {
        Reader {
            d: payload,
            pos: 0,
            le: true,
            aligned: false,
        }
    }

    #[inline]
    fn align(&mut self, n: usize) {
        if self.aligned {
            let m = self.pos % n;
            if m != 0 {
                self.pos += n - m;
            }
        }
    }

    #[inline]
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.d.len() {
            bail!(
                "buffer overrun: need {n} bytes at {} of {}",
                self.pos,
                self.d.len()
            );
        }
        let s = &self.d[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.d.len().saturating_sub(self.pos)
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    #[inline]
    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.take(1)?[0] as i8)
    }

    read_prim!(u16, u16, 2);
    read_prim!(i16, i16, 2);
    read_prim!(u32, u32, 4);
    read_prim!(i32, i32, 4);
    read_prim!(u64, u64, 8);
    read_prim!(i64, i64, 8);
    read_prim!(f32, f32, 4);
    read_prim!(f64, f64, 8);

    /// String payload bytes (without terminator).
    pub fn str_bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        if self.aligned {
            // CDR: length includes the NUL terminator
            let b = self.take(n)?;
            Ok(if n > 0 { &b[..n - 1] } else { b })
        } else {
            self.take(n)
        }
    }

    pub fn wstr(&mut self) -> Result<String> {
        let n = self.u32()? as usize;
        if n * 2 > self.remaining() {
            bail!("wstring length {n} exceeds remaining buffer");
        }
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(self.u16()?);
        }
        if v.last() == Some(&0) {
            v.pop();
        }
        Ok(String::from_utf16_lossy(&v))
    }
}

pub enum ColB {
    Bool(Vec<bool>),
    I8(Vec<i8>),
    U8(Vec<u8>),
    I16(Vec<i16>),
    U16(Vec<u16>),
    I32(Vec<i32>),
    U32(Vec<u32>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Str { offsets: Vec<i32>, data: Vec<u8> },
    Bin { offsets: Vec<i64>, data: Vec<u8> },
    FixedBin { n: usize, data: Vec<u8> },
    List { offsets: Vec<i32>, child: Box<ColB> },
    FixedList { n: usize, child: Box<ColB> },
    Struct { len: usize, children: Vec<ColB> },
}

pub fn new_builder(ft: &FieldType, defs: &[MsgDef]) -> ColB {
    match ft {
        FieldType::Prim(p) => match p {
            Prim::Bool => ColB::Bool(Vec::new()),
            Prim::I8 => ColB::I8(Vec::new()),
            Prim::U8 => ColB::U8(Vec::new()),
            Prim::I16 => ColB::I16(Vec::new()),
            Prim::U16 => ColB::U16(Vec::new()),
            Prim::I32 => ColB::I32(Vec::new()),
            Prim::U32 => ColB::U32(Vec::new()),
            Prim::I64 => ColB::I64(Vec::new()),
            Prim::U64 => ColB::U64(Vec::new()),
            Prim::F32 => ColB::F32(Vec::new()),
            Prim::F64 => ColB::F64(Vec::new()),
            Prim::Str | Prim::WStr => ColB::Str {
                offsets: vec![0],
                data: Vec::new(),
            },
        },
        FieldType::Complex(i) => ColB::Struct {
            len: 0,
            children: defs[*i]
                .fields
                .iter()
                .map(|(_, ft)| new_builder(ft, defs))
                .collect(),
        },
        FieldType::Array(inner, kind) => {
            if matches!(inner.as_ref(), FieldType::Prim(Prim::U8)) {
                return match kind {
                    ArrayKind::Fixed(n) => ColB::FixedBin {
                        n: *n,
                        data: Vec::new(),
                    },
                    ArrayKind::Unbounded => ColB::Bin {
                        offsets: vec![0],
                        data: Vec::new(),
                    },
                };
            }
            let child = Box::new(new_builder(inner, defs));
            match kind {
                ArrayKind::Fixed(n) => ColB::FixedList { n: *n, child },
                ArrayKind::Unbounded => ColB::List {
                    offsets: vec![0],
                    child,
                },
            }
        }
    }
}

pub fn col_len(b: &ColB) -> usize {
    match b {
        ColB::Bool(v) => v.len(),
        ColB::I8(v) => v.len(),
        ColB::U8(v) => v.len(),
        ColB::I16(v) => v.len(),
        ColB::U16(v) => v.len(),
        ColB::I32(v) => v.len(),
        ColB::U32(v) => v.len(),
        ColB::I64(v) => v.len(),
        ColB::U64(v) => v.len(),
        ColB::F32(v) => v.len(),
        ColB::F64(v) => v.len(),
        ColB::Str { offsets, .. } => offsets.len() - 1,
        ColB::Bin { offsets, .. } => offsets.len() - 1,
        ColB::FixedBin { n, data } => {
            if *n == 0 {
                0
            } else {
                data.len() / n
            }
        }
        ColB::List { offsets, .. } => offsets.len() - 1,
        ColB::FixedList { n, child } => {
            if *n == 0 {
                0
            } else {
                col_len(child) / n
            }
        }
        ColB::Struct { len, .. } => *len,
    }
}

pub fn append(ft: &FieldType, b: &mut ColB, r: &mut Reader, defs: &[MsgDef]) -> Result<()> {
    match (ft, b) {
        (FieldType::Prim(Prim::Bool), ColB::Bool(v)) => v.push(r.u8()? != 0),
        (FieldType::Prim(Prim::I8), ColB::I8(v)) => v.push(r.i8()?),
        (FieldType::Prim(Prim::U8), ColB::U8(v)) => v.push(r.u8()?),
        (FieldType::Prim(Prim::I16), ColB::I16(v)) => v.push(r.i16()?),
        (FieldType::Prim(Prim::U16), ColB::U16(v)) => v.push(r.u16()?),
        (FieldType::Prim(Prim::I32), ColB::I32(v)) => v.push(r.i32()?),
        (FieldType::Prim(Prim::U32), ColB::U32(v)) => v.push(r.u32()?),
        (FieldType::Prim(Prim::I64), ColB::I64(v)) => v.push(r.i64()?),
        (FieldType::Prim(Prim::U64), ColB::U64(v)) => v.push(r.u64()?),
        (FieldType::Prim(Prim::F32), ColB::F32(v)) => v.push(r.f32()?),
        (FieldType::Prim(Prim::F64), ColB::F64(v)) => v.push(r.f64()?),
        (FieldType::Prim(Prim::Str), ColB::Str { offsets, data }) => {
            let s = r.str_bytes()?;
            data.extend_from_slice(s);
            offsets.push(data.len() as i32);
        }
        (FieldType::Prim(Prim::WStr), ColB::Str { offsets, data }) => {
            let s = r.wstr()?;
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as i32);
        }
        (FieldType::Complex(i), ColB::Struct { len, children }) => {
            for ((_, fty), cb) in defs[*i].fields.iter().zip(children.iter_mut()) {
                append(fty, cb, r, defs)?;
            }
            *len += 1;
        }
        (FieldType::Array(_, _), ColB::Bin { offsets, data }) => {
            let n = r.u32()? as usize;
            let bytes = r.take(n)?;
            data.extend_from_slice(bytes);
            offsets.push(data.len() as i64);
        }
        (FieldType::Array(_, _), ColB::FixedBin { n, data }) => {
            let bytes = r.take(*n)?;
            data.extend_from_slice(bytes);
        }
        (FieldType::Array(inner, _), ColB::List { offsets, child }) => {
            let n = r.u32()? as usize;
            if n > r.remaining() {
                bail!("sequence length {n} exceeds remaining buffer");
            }
            for _ in 0..n {
                append(inner, child, r, defs)?;
            }
            offsets.push(col_len(child) as i32);
        }
        (FieldType::Array(inner, _), ColB::FixedList { n, child }) => {
            for _ in 0..*n {
                append(inner, child, r, defs)?;
            }
        }
        _ => bail!("internal error: builder/type mismatch"),
    }
    Ok(())
}

/// Consume the builder and produce an Arrow array; `ft` must be the type the
/// builder was created from.
pub fn finish(b: ColB, ft: &FieldType, defs: &[MsgDef]) -> ArrayRef {
    match b {
        ColB::Bool(v) => Arc::new(BooleanArray::from(v)),
        ColB::I8(v) => Arc::new(Int8Array::from(v)),
        ColB::U8(v) => Arc::new(UInt8Array::from(v)),
        ColB::I16(v) => Arc::new(Int16Array::from(v)),
        ColB::U16(v) => Arc::new(UInt16Array::from(v)),
        ColB::I32(v) => Arc::new(Int32Array::from(v)),
        ColB::U32(v) => Arc::new(UInt32Array::from(v)),
        ColB::I64(v) => Arc::new(Int64Array::from(v)),
        ColB::U64(v) => Arc::new(UInt64Array::from(v)),
        ColB::F32(v) => Arc::new(Float32Array::from(v)),
        ColB::F64(v) => Arc::new(Float64Array::from(v)),
        ColB::Str { offsets, data } => Arc::new(StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(data),
            None,
        )),
        ColB::Bin { offsets, data } => Arc::new(LargeBinaryArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(data),
            None,
        )),
        ColB::FixedBin { n, data } => Arc::new(
            FixedSizeBinaryArray::try_new(n as i32, Buffer::from_vec(data), None)
                .expect("fixed-size binary build"),
        ),
        ColB::List { offsets, child } => {
            let inner_ft = match ft {
                FieldType::Array(inner, _) => inner.as_ref(),
                _ => unreachable!("list builder with non-array type"),
            };
            let child_arr = finish(*child, inner_ft, defs);
            let field = Arc::new(Field::new("item", child_arr.data_type().clone(), false));
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                child_arr,
                None,
            ))
        }
        ColB::FixedList { n, child } => {
            let inner_ft = match ft {
                FieldType::Array(inner, _) => inner.as_ref(),
                _ => unreachable!("list builder with non-array type"),
            };
            let child_arr = finish(*child, inner_ft, defs);
            let field = Arc::new(Field::new("item", child_arr.data_type().clone(), false));
            Arc::new(FixedSizeListArray::new(field, n as i32, child_arr, None))
        }
        ColB::Struct { len, children } => {
            let def_idx = match ft {
                FieldType::Complex(i) => *i,
                _ => unreachable!("struct builder with non-complex type"),
            };
            if children.is_empty() {
                return Arc::new(StructArray::new_empty_fields(len, None));
            }
            let fields = crate::rosmsg::struct_fields(def_idx, defs);
            let arrays: Vec<ArrayRef> = children
                .into_iter()
                .zip(defs[def_idx].fields.iter())
                .map(|(cb, (_, fty))| finish(cb, fty, defs))
                .collect();
            Arc::new(StructArray::new(fields, arrays, None))
        }
    }
}
