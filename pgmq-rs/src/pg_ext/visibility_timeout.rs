use chrono::{DateTime, Duration, Utc};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::{Database, Decode, Encode, Postgres, Type};
use std::ops::Deref;

// todo: Update comment
/// Type to represent the `vt` (visibility timeout) timestamp column of a queue. Used by various
/// methods in [`crate::pg_ext::PGMQueueExt`] to set the visibility timeout of a job. Supports
/// converting from [`Duration`] and various integer types (assumed to be a duration in seconds) --
/// the timestamp value is determined by adding the provided duration to the current timestamp. This
/// type can also be used to interact with the `vt` column of a queue using `sqlx`.
///
/// # Examples
///
/// ## Convert from `i32`
/// ```
/// # use chrono::{DateTime, Duration, Utc};
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// let duration = 10i32;
/// let expected = Utc::now() + Duration::seconds(duration as i64);
/// let actual = VisibilityTimeoutOffset::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
///
/// ## Convert from `u32`
/// ```
/// # use chrono::{DateTime, Duration, Utc};
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// let duration = 10u32;
/// let expected = Utc::now() + Duration::seconds(duration as i64);
/// let actual = VisibilityTimeoutOffset::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
///
/// ## Convert from `i64`
/// ```
/// # use chrono::{DateTime, Duration, Utc};
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// let duration = 10i64;
/// let expected = Utc::now() + Duration::seconds(duration);
/// let actual = VisibilityTimeoutOffset::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
///
/// ## Convert from [`Duration`]`
/// ```
/// # use chrono::{DateTime, Duration, Utc};
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// let duration = Duration::seconds(10);
/// let expected = Utc::now() + duration;
/// let actual = VisibilityTimeoutOffset::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
#[derive(Clone, Copy, Debug)]
pub enum VisibilityTimeoutOffset {
    Chrono(Duration),
    Std(std::time::Duration),
}

impl From<i32> for VisibilityTimeoutOffset {
    fn from(value: i32) -> Self {
        Duration::seconds(value as i64).into()
    }
}

impl From<i64> for VisibilityTimeoutOffset {
    fn from(value: i64) -> Self {
        Duration::seconds(value).into()
    }
}

impl From<u32> for VisibilityTimeoutOffset {
    fn from(value: u32) -> Self {
        Duration::seconds(value as i64).into()
    }
}

impl From<Duration> for VisibilityTimeoutOffset {
    fn from(value: Duration) -> Self {
        Self::Chrono(value)
    }
}

impl From<std::time::Duration> for VisibilityTimeoutOffset {
    fn from(value: std::time::Duration) -> Self {
        Self::Std(value)
    }
}

// Todo: encode as float or interval (default is interval)?
impl<'q, DB: Database> Encode<'q, DB> for VisibilityTimeoutOffset
where
    Duration: Encode<'q, DB>,
    std::time::Duration: Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        match self {
            VisibilityTimeoutOffset::Chrono(value) => {
                <Duration as sqlx::Encode<'_, DB>>::encode_by_ref(&value, buf)
            }
            VisibilityTimeoutOffset::Std(value) => {
                <std::time::Duration as sqlx::Encode<'_, DB>>::encode_by_ref(&value, buf)
            }
        }
    }
}

impl<DB: Database> Type<DB> for VisibilityTimeoutOffset
where
    Duration: Type<DB>,
    std::time::Duration: Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        debug_assert_eq!(
            <Duration as sqlx::Type<DB>>::type_info(),
            <std::time::Duration as sqlx::Type<DB>>::type_info()
        );
        <Duration as sqlx::Type<DB>>::type_info()
    }
}
