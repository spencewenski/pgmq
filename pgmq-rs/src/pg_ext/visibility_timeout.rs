use chrono::{DateTime, Duration, Utc};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::{Database, Decode, Encode, Type};
use std::ops::Deref;

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
/// # use pgmq::pg_ext::VisibilityTimeout;
/// let duration = 10i32;
/// let expected = Utc::now() + Duration::seconds(duration as i64);
/// let actual = VisibilityTimeout::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
///
/// ## Convert from `u32`
/// ```
/// # use chrono::{DateTime, Duration, Utc};
/// # use pgmq::pg_ext::VisibilityTimeout;
/// let duration = 10u32;
/// let expected = Utc::now() + Duration::seconds(duration as i64);
/// let actual = VisibilityTimeout::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
///
/// ## Convert from `i64`
/// ```
/// # use chrono::{DateTime, Duration, Utc};
/// # use pgmq::pg_ext::VisibilityTimeout;
/// let duration = 10i64;
/// let expected = Utc::now() + Duration::seconds(duration);
/// let actual = VisibilityTimeout::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
///
/// ## Convert from [`Duration`]`
/// ```
/// # use chrono::{DateTime, Duration, Utc};
/// # use pgmq::pg_ext::VisibilityTimeout;
/// let duration = Duration::seconds(10);
/// let expected = Utc::now() + duration;
/// let actual = VisibilityTimeout::from(duration);
/// assert!(*actual - expected < Duration::milliseconds(1))
/// ```
#[derive(Clone, Copy, Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct VisibilityTimeout(pub DateTime<Utc>);

impl AsRef<DateTime<Utc>> for VisibilityTimeout {
    fn as_ref(&self) -> &DateTime<Utc> {
        &self.0
    }
}

impl Deref for VisibilityTimeout {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<i32> for VisibilityTimeout {
    fn from(value: i32) -> Self {
        Duration::seconds(value as i64).into()
    }
}

impl From<i64> for VisibilityTimeout {
    fn from(value: i64) -> Self {
        Duration::seconds(value).into()
    }
}

impl From<u32> for VisibilityTimeout {
    fn from(value: u32) -> Self {
        Duration::seconds(value as i64).into()
    }
}

impl From<Duration> for VisibilityTimeout {
    fn from(value: Duration) -> Self {
        Self(Utc::now() + value)
    }
}

impl From<DateTime<Utc>> for VisibilityTimeout {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value)
    }
}
