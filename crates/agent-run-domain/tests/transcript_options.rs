//! Shared transcript option decoding and validation contracts.

use agent_run_domain::transcript::{TranscriptQuery, TranscriptView};

/// Decodes only the two serialized names; other spellings are typed errors.
#[test]
fn transcript_view_decodes_only_known_names() {
    assert_eq!(
        "raw".parse::<TranscriptView>().unwrap(),
        TranscriptView::Raw
    );
    assert_eq!(
        "blocks".parse::<TranscriptView>().unwrap(),
        TranscriptView::Blocks
    );
    for spelling in ["block", "RAW", "", "rows"] {
        assert!(spelling.parse::<TranscriptView>().is_err(), "{spelling}");
    }
}

/// The compatible default is a forward raw page of the historical size.
#[test]
fn default_query_is_a_forward_raw_page() {
    let query = TranscriptQuery::default();
    assert_eq!(query.cursor, 0);
    assert_eq!(query.limit, 200);
    assert_eq!(query.view, TranscriptView::Raw);
    assert!(query.tail_blocks.is_none());
    assert!(query.before_cursor.is_none());
    assert!(query.validate().is_ok());
}

/// Bounds and selector conflicts reject before any read.
#[test]
fn query_validation_rejects_conflicting_selections() {
    assert!(
        TranscriptQuery {
            limit: 200,
            view: TranscriptView::Blocks,
            ..Default::default()
        }
        .validate()
        .is_ok()
    );
    for invalid in [
        TranscriptQuery {
            cursor: -1,
            ..Default::default()
        },
        TranscriptQuery {
            limit: 0,
            ..Default::default()
        },
        TranscriptQuery {
            limit: 1001,
            ..Default::default()
        },
        TranscriptQuery {
            tail_blocks: Some(0),
            view: TranscriptView::Blocks,
            ..Default::default()
        },
        TranscriptQuery {
            before_cursor: Some(-3),
            view: TranscriptView::Blocks,
            ..Default::default()
        },
        TranscriptQuery {
            view: TranscriptView::Raw,
            tail_blocks: Some(2),
            ..Default::default()
        },
    ] {
        assert!(invalid.validate().is_err(), "{invalid:?}");
    }
}
