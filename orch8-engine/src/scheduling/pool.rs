use chrono::NaiveDate;
use rand::seq::SliceRandom;
use rand::Rng;

use orch8_types::pool::{PoolAssignment, PoolResource, RotationStrategy};

/// Select the next resource from a pool based on the rotation strategy.
/// Updates `round_robin_index` for round-robin strategy (caller must persist).
pub fn select_resource(
    resources: &[PoolResource],
    strategy: RotationStrategy,
    round_robin_index: &mut u32,
    today: NaiveDate,
) -> PoolAssignment {
    let available: Vec<&PoolResource> = resources
        .iter()
        .filter(|r| r.enabled && r.has_capacity(today))
        .collect();

    if resources.iter().all(|r| !r.enabled) {
        return PoolAssignment::Empty;
    }

    if available.is_empty() {
        return PoolAssignment::Exhausted;
    }

    match strategy {
        RotationStrategy::RoundRobin => {
            let idx = (*round_robin_index as usize) % available.len();
            *round_robin_index = round_robin_index.wrapping_add(1);
            PoolAssignment::Assigned(available[idx].resource_key.clone())
        }
        RotationStrategy::Weighted => {
            let total_weight: u32 = available.iter().map(|r| r.weight).sum();
            if total_weight == 0 {
                return PoolAssignment::Empty;
            }
            let mut pick = rand::thread_rng().gen_range(0..total_weight);
            for r in &available {
                if pick < r.weight {
                    return PoolAssignment::Assigned(r.resource_key.clone());
                }
                pick -= r.weight;
            }
            // Fallback (shouldn't reach, but avoid panic)
            available
                .first()
                .map_or(PoolAssignment::Empty, |r| {
                    PoolAssignment::Assigned(r.resource_key.clone())
                })
        }
        RotationStrategy::Random => match available.choose(&mut rand::thread_rng()) {
            Some(r) => PoolAssignment::Assigned(r.resource_key.clone()),
            None => PoolAssignment::Empty,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_types::ids::ResourceKey;
    use uuid::Uuid;

    fn make_resource(key: &str, weight: u32, daily_cap: u32) -> PoolResource {
        PoolResource {
            id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
            resource_key: ResourceKey(key.into()),
            name: key.into(),
            weight,
            enabled: true,
            daily_cap,
            daily_usage: 0,
            daily_usage_date: None,
            warmup_start: None,
            warmup_days: 0,
            warmup_start_cap: 0,
            created_at: Utc::now(),
        }
    }

    fn today() -> NaiveDate {
        Utc::now().date_naive()
    }

    #[test]
    fn round_robin_cycles() {
        let resources = vec![
            make_resource("a", 1, 0),
            make_resource("b", 1, 0),
            make_resource("c", 1, 0),
        ];
        let mut idx = 0u32;

        let keys: Vec<String> = (0..6)
            .map(|_| {
                match select_resource(&resources, RotationStrategy::RoundRobin, &mut idx, today()) {
                    PoolAssignment::Assigned(k) => k.0,
                    _ => panic!("expected assigned"),
                }
            })
            .collect();

        assert_eq!(keys, vec!["a", "b", "c", "a", "b", "c"]);
    }

    #[test]
    fn exhausted_when_all_at_cap() {
        let mut resources = vec![make_resource("a", 1, 5), make_resource("b", 1, 5)];
        let t = today();
        for r in &mut resources {
            r.daily_usage = 5;
            r.daily_usage_date = Some(t);
        }
        let mut idx = 0;
        assert!(matches!(
            select_resource(&resources, RotationStrategy::RoundRobin, &mut idx, t),
            PoolAssignment::Exhausted
        ));
    }

    #[test]
    fn empty_when_none_enabled() {
        let mut resources = vec![make_resource("a", 1, 0)];
        resources[0].enabled = false;
        let mut idx = 0;
        assert!(matches!(
            select_resource(&resources, RotationStrategy::RoundRobin, &mut idx, today()),
            PoolAssignment::Empty
        ));
    }

    #[test]
    fn random_returns_available() {
        let resources = vec![make_resource("a", 1, 0), make_resource("b", 1, 0)];
        let mut idx = 0;
        for _ in 0..20 {
            assert!(matches!(
                select_resource(&resources, RotationStrategy::Random, &mut idx, today()),
                PoolAssignment::Assigned(_)
            ));
        }
    }

    #[test]
    fn weighted_favors_higher_weight() {
        let resources = vec![make_resource("heavy", 100, 0), make_resource("light", 1, 0)];
        let mut idx = 0;
        let mut heavy_count = 0;
        for _ in 0..1000 {
            if let PoolAssignment::Assigned(k) =
                select_resource(&resources, RotationStrategy::Weighted, &mut idx, today())
            {
                if k.0 == "heavy" {
                    heavy_count += 1;
                }
            }
        }
        // Heavy should get ~99% of assignments
        assert!(heavy_count > 900, "heavy got {heavy_count}/1000");
    }

    #[test]
    fn skips_over_cap_resources() {
        let mut resources = vec![make_resource("a", 1, 5), make_resource("b", 1, 0)];
        let t = today();
        resources[0].daily_usage = 5;
        resources[0].daily_usage_date = Some(t);
        let mut idx = 0;
        // Only "b" should be picked (round robin with 1 available)
        match select_resource(&resources, RotationStrategy::RoundRobin, &mut idx, t) {
            PoolAssignment::Assigned(k) => assert_eq!(k.0, "b"),
            other => panic!("expected assigned, got {other:?}"),
        }
    }
}
