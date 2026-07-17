//! Translate an instance [`Snapshot`] into the `InstanceStatus` the operator
//! polls. Kept pure so the mapping is unit-tested without a database.

use crate::instance::Snapshot;
use pgshard_proto::v1;

pub fn to_status(s: &Snapshot) -> v1::InstanceStatus {
    let role = if s.in_recovery {
        v1::InstanceRole::Standby
    } else {
        v1::InstanceRole::Primary
    };
    v1::InstanceStatus {
        role: role as i32,
        // A fenced instance is held down, so it is never ready even if the last
        // observed state accepted connections.
        ready: s.accepting && !s.fenced,
        timeline: s.timeline,
        wal_write_lsn: Some(v1::Lsn { value: s.write_lsn }),
        wal_receive_lsn: Some(v1::Lsn {
            value: s.receive_lsn,
        }),
        wal_replay_lsn: Some(v1::Lsn {
            value: s.replay_lsn,
        }),
        replay_lag_seconds: 0.0,
        sync_state: v1::SyncState::Unspecified as i32,
        wal_receiver_active: s.receiver_active,
        archiving_healthy: false,
        postgres_version: s.postgres_version.clone(),
        system_id: s.system_id,
        fenced: s.fenced,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_maps_to_primary_role() {
        let s = Snapshot {
            in_recovery: false,
            accepting: true,
            timeline: 3,
            write_lsn: 42,
            ..Default::default()
        };
        let st = to_status(&s);
        assert_eq!(st.role, v1::InstanceRole::Primary as i32);
        assert!(st.ready);
        assert_eq!(st.timeline, 3);
        assert_eq!(st.wal_write_lsn.unwrap().value, 42);
    }

    #[test]
    fn standby_maps_to_standby_role() {
        let s = Snapshot {
            in_recovery: true,
            accepting: true,
            receive_lsn: 100,
            receiver_active: true,
            ..Default::default()
        };
        let st = to_status(&s);
        assert_eq!(st.role, v1::InstanceRole::Standby as i32);
        assert!(st.wal_receiver_active);
        assert_eq!(st.wal_receive_lsn.unwrap().value, 100);
    }

    #[test]
    fn fenced_is_never_ready() {
        let s = Snapshot {
            in_recovery: false,
            accepting: true,
            fenced: true,
            ..Default::default()
        };
        let st = to_status(&s);
        assert!(!st.ready);
        assert!(st.fenced);
    }
}
