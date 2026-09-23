/// Find the closest match for `input` among `candidates`.
///
/// Uses normalized Levenshtein distance. Returns `Some(candidate)` if the
/// best match is within a reasonable threshold (distance ≤ 40% of the
/// longer string), `None` otherwise.
pub fn did_you_mean<'a>(input: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let input_lower = input.to_lowercase();
    let input_chars = input_lower.chars().count();
    candidates
        .iter()
        .map(|c| {
            let candidate_lower = c.to_lowercase();
            let dist = strsim::levenshtein(&input_lower, &candidate_lower);
            let max_chars = input_chars.max(candidate_lower.chars().count());
            (c, dist, max_chars)
        })
        .filter(|(_, dist, max_chars)| {
            // Only suggest if the distance is within 40% of the longer string.
            *max_chars > 0 && *dist <= (max_chars * 2 / 5).max(1)
        })
        .min_by_key(|(_, dist, _)| *dist)
        .map(|(c, _, _)| *c)
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATES: &[&str] = &[
        "scheduled",
        "running",
        "waiting",
        "paused",
        "completed",
        "failed",
        "cancelled",
    ];

    #[test]
    fn exact_match_returns_none_because_caller_handles_exact() {
        // Exact matches are handled by the caller's match arm, not here.
        // But if called with an exact match, it should still return it.
        assert_eq!(did_you_mean("running", STATES), Some("running"));
    }

    #[test]
    fn typo_single_char() {
        assert_eq!(did_you_mean("runing", STATES), Some("running"));
    }

    #[test]
    fn typo_transposition() {
        assert_eq!(did_you_mean("runnign", STATES), Some("running"));
    }

    #[test]
    fn typo_missing_char() {
        assert_eq!(did_you_mean("faild", STATES), Some("failed"));
    }

    #[test]
    fn typo_extra_char() {
        assert_eq!(did_you_mean("scheduledd", STATES), Some("scheduled"));
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(did_you_mean("Running", STATES), Some("running"));
    }

    #[test]
    fn completely_wrong_returns_none() {
        assert_eq!(did_you_mean("foobar", STATES), None);
    }

    #[test]
    fn empty_input_returns_none() {
        assert_eq!(did_you_mean("", STATES), None);
    }

    #[test]
    fn empty_candidates_returns_none() {
        assert_eq!(did_you_mean("running", &[]), None);
    }

    #[test]
    fn short_input_with_close_match() {
        assert_eq!(did_you_mean("rue", &["run", "red", "blue"]), Some("run"));
    }

    #[test]
    fn multibyte_candidates_use_character_not_byte_thresholds() {
        assert_eq!(did_you_mean("あいうえお", &["かきくけこ"]), None);
        assert_eq!(
            did_you_mean("あいうえお", &["あいうえか"]),
            Some("あいうえか")
        );
    }

    #[test]
    fn candidate_case_is_normalized_without_changing_returned_spelling() {
        assert_eq!(did_you_mean("café", &["CAFÉ"]), Some("CAFÉ"));
    }
}
