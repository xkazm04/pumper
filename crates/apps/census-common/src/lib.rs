//! Shared helpers for the Census API apps (`census-density`, `census-nonemp`).
//!
//! These were duplicated verbatim in both apps, which is precisely how they
//! drifted: the disclosure/jam-sentinel guard was applied in one parser and
//! forgotten in another, silently summing `-666666666` into national totals.
//! One definition each, used by both, so a fix can't land in only half the fleet.

use pumper_core::{AppContext, Error, Result};
use serde_json::Value;

/// Parses a Census numeric cell.
///
/// Missing, non-numeric, and **negative** values are treated as suppressed
/// (`None`) rather than data: Census encodes disclosure suppression and jam
/// values as negative sentinels (e.g. `-666666666`), so parsing them as real
/// numbers corrupts every total they reach.
pub fn census_num(cell: Option<&String>) -> Option<i64> {
    cell.and_then(|s| s.trim().parse::<i64>().ok()).filter(|v| *v >= 0)
}

/// Resolves the free Census API key: `params.api_key`, else env
/// `CENSUS_API_KEY`. `app` names the caller in the error so the operator knows
/// which app is asking.
pub fn api_key(ctx: &AppContext, app: &str) -> Result<String> {
    ctx.params
        .get("api_key")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| std::env::var("CENSUS_API_KEY").ok())
        .filter(|k| !k.trim().is_empty())
        .ok_or_else(|| {
            Error::App(format!(
                "{app} needs a free Census API key — set env CENSUS_API_KEY or pass \
                 params.api_key. Get one instantly at \
                 https://api.census.gov/data/key_signup.html"
            ))
        })
}

/// USPS abbreviation for a state FIPS code; an unknown code passes through
/// unchanged so unexpected geographies stay traceable rather than becoming "??".
pub fn state_abbr(fips: &str) -> &str {
    match fips {
        "01" => "AL", "02" => "AK", "04" => "AZ", "05" => "AR", "06" => "CA",
        "08" => "CO", "09" => "CT", "10" => "DE", "11" => "DC", "12" => "FL",
        "13" => "GA", "15" => "HI", "16" => "ID", "17" => "IL", "18" => "IN",
        "19" => "IA", "20" => "KS", "21" => "KY", "22" => "LA", "23" => "ME",
        "24" => "MD", "25" => "MA", "26" => "MI", "27" => "MN", "28" => "MS",
        "29" => "MO", "30" => "MT", "31" => "NE", "32" => "NV", "33" => "NH",
        "34" => "NJ", "35" => "NM", "36" => "NY", "37" => "NC", "38" => "ND",
        "39" => "OH", "40" => "OK", "41" => "OR", "42" => "PA", "44" => "RI",
        "45" => "SC", "46" => "SD", "47" => "TN", "48" => "TX", "49" => "UT",
        "50" => "VT", "51" => "VA", "53" => "WA", "54" => "WV", "55" => "WI",
        "56" => "WY", "72" => "PR",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{census_num, state_abbr};

    #[test]
    fn census_num_rejects_suppression_sentinels() {
        assert_eq!(census_num(Some(&"1234".to_string())), Some(1234));
        assert_eq!(census_num(Some(&" 0 ".to_string())), Some(0));
        // Negative jam/annotation sentinels are suppression, not data.
        assert_eq!(census_num(Some(&"-666666666".to_string())), None);
        assert_eq!(census_num(Some(&"-1".to_string())), None);
        // Missing / non-numeric cells are suppressed too.
        assert_eq!(census_num(Some(&"".to_string())), None);
        assert_eq!(census_num(Some(&"D".to_string())), None);
        assert_eq!(census_num(None), None);
    }

    #[test]
    fn state_abbr_maps_fips_and_passes_unknown_through() {
        assert_eq!(state_abbr("06"), "CA");
        assert_eq!(state_abbr("72"), "PR");
        assert_eq!(state_abbr("99"), "99");
    }
}
