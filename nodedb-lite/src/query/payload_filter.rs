// SPDX-License-Identifier: Apache-2.0

//! `PayloadAtom` list → `MetadataFilter`, the brute-force form Lite's vector
//! search evaluates against each candidate's stored payload row. Lite keeps
//! no payload bitmap index, so the atoms a planner emits for one are
//! evaluated row by row instead.

use nodedb_types::PayloadAtom;
use nodedb_types::filter::MetadataFilter;

use crate::error::LiteError;

/// The conjunction of `atoms`, or `None` when there are none.
pub(crate) fn payload_atoms_to_metadata(
    atoms: &[PayloadAtom],
) -> Result<Option<MetadataFilter>, LiteError> {
    let mut parts = Vec::with_capacity(atoms.len());
    for atom in atoms {
        parts.push(atom_to_metadata(atom)?);
    }
    Ok(match parts.len() {
        0 => None,
        1 => parts.pop(),
        _ => Some(MetadataFilter::And(parts)),
    })
}

fn atom_to_metadata(atom: &PayloadAtom) -> Result<MetadataFilter, LiteError> {
    match atom {
        PayloadAtom::Eq(field, value) => Ok(MetadataFilter::Eq {
            field: field.clone(),
            value: value.clone(),
        }),
        PayloadAtom::In(field, values) => Ok(MetadataFilter::In {
            field: field.clone(),
            values: values.clone(),
        }),
        PayloadAtom::Range {
            field,
            low,
            low_inclusive,
            high,
            high_inclusive,
        } => {
            let mut parts = Vec::with_capacity(2);
            if let Some(lo) = low {
                parts.push(if *low_inclusive {
                    MetadataFilter::Gte {
                        field: field.clone(),
                        value: lo.clone(),
                    }
                } else {
                    MetadataFilter::Gt {
                        field: field.clone(),
                        value: lo.clone(),
                    }
                });
            }
            if let Some(hi) = high {
                parts.push(if *high_inclusive {
                    MetadataFilter::Lte {
                        field: field.clone(),
                        value: hi.clone(),
                    }
                } else {
                    MetadataFilter::Lt {
                        field: field.clone(),
                        value: hi.clone(),
                    }
                });
            }
            match parts.len() {
                0 => Err(LiteError::BadRequest {
                    detail: format!("payload range on '{field}' has no bounds"),
                }),
                1 => Ok(parts.remove(0)),
                _ => Ok(MetadataFilter::And(parts)),
            }
        }
        // `PayloadAtom` is `#[non_exhaustive]` in nodedb-types: an atom kind
        // this build does not know is refused, never dropped from the filter.
        other => Err(LiteError::BadRequest {
            detail: format!("payload filter atom not supported by this build: {other:?}"),
        }),
    }
}

/// AND two optional filters.
pub(crate) fn and_metadata(
    a: Option<MetadataFilter>,
    b: Option<MetadataFilter>,
) -> Option<MetadataFilter> {
    match (a, b) {
        (None, None) => None,
        (Some(f), None) | (None, Some(f)) => Some(f),
        (Some(a), Some(b)) => Some(MetadataFilter::And(vec![a, b])),
    }
}
