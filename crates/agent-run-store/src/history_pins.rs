//! Permanent pin keys and bounded operator observation for retained lineages.

use crate::Store;
use agent_run_domain::{
    Error, Result, StoreId,
    batch::{
        HistoryPin, HistoryPinFilter, HistoryPinRelease, HistoryPinState, HistoryPinsPage, PinId,
    },
    error::invalid,
};
use rusqlite::{OptionalExtension, params};

impl Store<'_> {
    /// Lists matching pins in exclusive key order without joining retained history.
    pub fn history_pins(
        &self,
        filter: HistoryPinFilter,
        limit: usize,
        after: Option<&PinId>,
    ) -> Result<HistoryPinsPage> {
        self.history_pins_scoped(filter, limit, after, None)
    }

    /// Lists a page with optional identity checked in the same read snapshot.
    pub fn history_pins_scoped(
        &self,
        filter: HistoryPinFilter,
        limit: usize,
        after: Option<&PinId>,
        expected: Option<&StoreId>,
    ) -> Result<HistoryPinsPage> {
        if !(1..=1000).contains(&limit) {
            return Err(invalid("history pin limit must be 1..=1000"));
        }
        let state = match filter {
            HistoryPinFilter::All => None,
            HistoryPinFilter::Active => Some(0),
            HistoryPinFilter::Released => Some(1),
        };
        let tx = self.conn.unchecked_transaction()?;
        if expected.is_some() {
            crate::store_identity::require(&tx, expected)?;
        }
        let mut statement = tx.prepare(
            "SELECT pin_id,root_agent_id,released FROM history_pins \
             WHERE (?1 IS NULL OR pin_id>?1) AND (?2 IS NULL OR released=?2) ORDER BY pin_id LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![after.map(PinId::as_str), state, (limit + 1) as i64],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?;
        let mut pins = Vec::new();
        for row in rows {
            let (pin_id, agent_id, released) = row?;
            pins.push(HistoryPin {
                pin_id: pin_id
                    .parse()
                    .map_err(|_| Error::Integrity("invalid stored pin identity".into()))?,
                agent_id: agent_id
                    .parse()
                    .map_err(|_| Error::Integrity("invalid stored pin root".into()))?,
                status: match released {
                    0 => HistoryPinState::Active,
                    1 => HistoryPinState::Released,
                    _ => return Err(Error::Integrity("invalid stored pin state".into())),
                },
            });
        }
        let more = pins.len() > limit;
        if more {
            pins.pop();
        }
        let next_after_pin_id = if more {
            pins.last().map(|pin| pin.pin_id.clone())
        } else {
            None
        };
        drop(statement);
        tx.commit()?;
        Ok(HistoryPinsPage {
            pins,
            next_after_pin_id,
        })
    }

    /// Releases a known pin once; its permanent tombstone is never reactivated.
    pub fn release_history_pin(&mut self, pin_id: &PinId) -> Result<Option<HistoryPinRelease>> {
        self.release_history_pin_scoped(pin_id, None)
    }

    /// Checks database scope and conflicts while any active hold protects the exact pin.
    pub fn release_history_pin_scoped(
        &mut self,
        pin_id: &PinId,
        expected: Option<&StoreId>,
    ) -> Result<Option<HistoryPinRelease>> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        if expected.is_some() {
            crate::store_identity::require(&tx, expected)?;
        }
        let state: Option<i64> = tx
            .query_row(
                "SELECT released FROM history_pins WHERE pin_id=?",
                [pin_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(state) = state else {
            tx.commit()?;
            return Ok(None);
        };
        let already_released = match state {
            0 => false,
            1 => true,
            _ => return Err(Error::Integrity("invalid stored pin state".into())),
        };
        if !already_released {
            crate::lineage_holds::require_pin_unheld(&tx, pin_id)?;
            let changed = tx.execute(
                "UPDATE history_pins SET released=1 WHERE pin_id=? AND released=0",
                [pin_id.as_str()],
            )?;
            if changed != 1 {
                return Err(Error::Integrity("history pin release lost its row".into()));
            }
        }
        tx.commit()?;
        Ok(Some(HistoryPinRelease {
            pin_id: pin_id.clone(),
            status: HistoryPinState::Released,
            already_released,
        }))
    }
}
