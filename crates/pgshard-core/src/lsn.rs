use std::fmt;

use thiserror::Error;

/// PostgreSQL log sequence number. Displays in the native `X/X` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid LSN {0:?}: expected <hex>/<hex>")]
pub struct InvalidLsn(String);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 as u32)
    }
}

impl std::str::FromStr for Lsn {
    type Err = InvalidLsn;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidLsn(s.to_string());
        let (hi, lo) = s.split_once('/').ok_or_else(invalid)?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| invalid())?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| invalid())?;
        if hi > u64::from(u32::MAX) || lo > u64::from(u32::MAX) {
            return Err(invalid());
        }
        Ok(Lsn(hi << 32 | lo))
    }
}

#[cfg(test)]
mod tests {
    use super::Lsn;

    #[test]
    fn round_trips_postgres_format() {
        let lsn: Lsn = "16/B374D848".parse().unwrap();
        assert_eq!(lsn.0, 0x16_B374_D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert!("nope".parse::<Lsn>().is_err());
        assert!("1/123456789".parse::<Lsn>().is_err());
    }
}
