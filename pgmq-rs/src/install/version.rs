use crate::install::install_err;
use crate::PgmqError;
use regex::Regex;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// The `pgmq` extension control file. Used to determine which version of `pgmq` would be
/// installed by running the `pgmq.sql` script.
const EXTENSION_CONFIG: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../pgmq-extension/pgmq.control"
));

/// Struct to represent a basic semver version, e.g. `1.2.3`.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Version {
    /// The first segment of the version string, e.g., for version `1.2.3`, this would be set to `1`
    pub major: u32,
    /// The second segment of the version string, e.g., for version `1.2.3`, this would be set to `2`
    pub minor: u32,
    /// The third segment of the version string, e.g., for version `1.2.3`, this would be set to `3`
    pub patch: u32,
}

impl Version {
    /// Get the current version of the `pgmq` installation scripts that are embedded in the crate.
    pub fn get_pgmq_version() -> Result<Self, PgmqError> {
        Self::get_pgmq_version_from_contents(EXTENSION_CONFIG)
    }

    /// Get the current version of the `pgmq` installation scripts that are embedded in the crate.
    ///
    /// * `extension_config`: The contents of the `pgmq.control` extension config file.
    fn get_pgmq_version_from_contents(extension_config: &str) -> Result<Self, PgmqError> {
        let version_line = extension_config
            .lines()
            .find(|line| line.trim_start().starts_with("default_version"))
            .ok_or_else(|| install_err("Version is not present in extension config"))?;
        let re =
            Regex::new(r"^\s*default_version\s*=\s*'(?<version>.*)'\s*$").map_err(install_err)?;
        let captures = re.captures(version_line).ok_or_else(|| {
            format!(
                "Unable to extract version from extension config: {}",
                version_line
            )
        })?;
        Self::from_str(&captures["version"])
    }
}

impl FromStr for Version {
    type Err = PgmqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = Regex::new(r"(?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)").map_err(install_err)?;
        let captures = re
            .captures(s)
            .ok_or_else(|| install_err(format!("Invalid script name: {}", s)))?;
        Ok(Self {
            major: u32::from_str(&captures["major"]).map_err(install_err)?,
            minor: u32::from_str(&captures["minor"]).map_err(install_err)?,
            patch: u32::from_str(&captures["patch"]).map_err(install_err)?,
        })
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.major.cmp(&other.major);
        match cmp {
            Ordering::Less | Ordering::Greater => {
                return cmp;
            }
            Ordering::Equal => {}
        }

        let cmp = self.minor.cmp(&other.minor);
        match cmp {
            Ordering::Less | Ordering::Greater => {
                return cmp;
            }
            Ordering::Equal => {}
        }

        self.patch.cmp(&other.patch)
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::Version;
    use insta::assert_debug_snapshot;
    use itertools::Itertools;
    use std::str::FromStr;

    #[test]
    fn from_str() {
        let version = Version::from_str("1.11.0").unwrap();
        assert_eq!(
            version,
            Version {
                major: 1,
                minor: 11,
                patch: 0
            }
        );
    }

    #[test]
    fn from_str_err() {
        let version = Version::from_str("invalid.version");
        assert!(version.is_err());
        let version = Version::from_str("a.b.c");
        assert!(version.is_err());
    }

    #[test]
    fn get_pgmq_version() {
        let extension_config = r"default_version = '1.11.0'";
        let pgmq_version = Version::get_pgmq_version_from_contents(extension_config).unwrap();
        assert_eq!(pgmq_version, Version::from_str("1.11.0").unwrap());
    }

    #[test]
    fn get_pgmq_version_extra_whitespace() {
        let extension_config = r"    default_version   =    '1.11.0'    ";
        let pgmq_version = Version::get_pgmq_version_from_contents(extension_config).unwrap();
        assert_eq!(pgmq_version, Version::from_str("1.11.0").unwrap());
    }

    #[test]
    fn get_pgmq_version_no_whitespace() {
        let extension_config = r"default_version='1.11.0'";
        let pgmq_version = Version::get_pgmq_version_from_contents(extension_config).unwrap();
        assert_eq!(pgmq_version, Version::from_str("1.11.0").unwrap());
    }

    #[test]
    fn get_pgmq_version_actual_config_file() {
        let version = Version::get_pgmq_version();
        // Don't check for a specific version, just check that the version was successfully parsed.
        // Otherwise, this test will fail every time the version is updated.
        assert!(version.is_ok());
    }

    #[test]
    fn sort() {
        let versions = [
            "0.1.0", "1.1.1", "2.0.1", "2.0.0", "2.0.0", "1.11.1", "1.0.1",
        ]
        .iter()
        .map(|version| Version::from_str(version).unwrap())
        .sorted()
        .unique()
        .collect::<Vec<Version>>();
        assert_debug_snapshot!(versions);
    }
}
