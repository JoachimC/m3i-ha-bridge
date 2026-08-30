//! How long to wait between attempts of the Bluetooth reader.

use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, PartialEq)]
pub enum Wait {
    Finished,
    Cancelled,
}

/// Decides the pause before the next attempt.
pub trait RetryDelay {
    /// Waits before the next attempt. `last_attempt` is the duration of the
    /// attempt that just ended. With it, the strategy can separate a fresh
    /// failure after hours of health from the next failure in a sequence.
    fn wait(
        &mut self,
        last_attempt: Duration,
        cancel_token: CancellationToken,
    ) -> impl Future<Output = Wait> + Send;
}

/// Exponential backoff with jitter.
///
/// A fixed short delay is correct while a failure looks transient. But the
/// adapter-missing case never resolves on its own: the bridge logs an error,
/// waits, fails identically, and repeats forever. At five seconds that is
/// seventeen thousand identical log lines each day, on a device that keeps
/// its journal on an SD card. The delay doubles up to a cap. This keeps the
/// first retry fast, which is what matters for a transient fault, and makes
/// a permanent fault cheap until someone can examine it.
pub struct Backoff {
    initial: Duration,
    max: Duration,
    /// An attempt that ran at least this long used a working adapter, so the
    /// fault is a fresh problem, not a continuation of an earlier one. The
    /// schedule then restarts and does not inherit a backoff from hours ago.
    /// The adapter-missing case fails in milliseconds and so never qualifies;
    /// that exclusion is the purpose of this threshold.
    healthy_after: Duration,
    /// Fraction of each delay that jitter may remove, e.g. `0.1` for "up to
    /// 10% shorter".
    jitter: f64,
    /// Delay for the next `wait`, before jitter.
    next: Duration,
    rng: u64,
}

/// Jitter removes up to 10% of each delay. A single bridge causes no
/// thundering herd, but jitter prevents retries in lockstep with other
/// processes that restart on the same timer — bluetoothd, most obviously.
const DEFAULT_JITTER: f64 = 0.1;

impl Backoff {
    pub fn new(initial: Duration, max: Duration, healthy_after: Duration) -> Self {
        Self::with_seed(initial, max, healthy_after, DEFAULT_JITTER, random_seed())
    }

    fn with_seed(
        initial: Duration,
        max: Duration,
        healthy_after: Duration,
        jitter: f64,
        seed: u64,
    ) -> Self {
        Self {
            initial,
            max,
            healthy_after,
            jitter,
            next: initial,
            rng: nonzero_seed(seed),
        }
    }

    /// Returns the delay after the attempt that just ended, and advances the
    /// schedule.
    fn next_delay(&mut self, last_attempt: Duration) -> Duration {
        if last_attempt >= self.healthy_after {
            self.next = self.initial;
        }
        let base = self.next;
        self.next = base.saturating_mul(2).min(self.max);
        apply_jitter(base, self.jitter, next_random(&mut self.rng))
    }
}

impl RetryDelay for Backoff {
    async fn wait(&mut self, last_attempt: Duration, cancel_token: CancellationToken) -> Wait {
        let delay = self.next_delay(last_attempt);
        tokio::select! {
            _ = cancel_token.cancelled() => Wait::Cancelled,
            _ = tokio::time::sleep(delay) => Wait::Finished,
        }
    }
}

/// A zero seed would make xorshift produce nothing but zeroes.
fn nonzero_seed(seed: u64) -> u64 {
    seed | 1
}

/// Shortens `base` by up to `fraction` of itself.
///
/// Jitter only subtracts, so a jittered delay can never exceed the cap or
/// the scheduled delay. This property is worth keeping, because "at most
/// `max`" is easier to reason about than "roughly `max`".
fn apply_jitter(base: Duration, fraction: f64, random: u64) -> Duration {
    if fraction <= 0.0 {
        return base;
    }
    base.mul_f64(1.0 - fraction.clamp(0.0, 1.0) * unit_interval(random))
}

/// A uniform value in [0, 1) from the top 53 bits — the size of an f64
/// mantissa — so the conversion to f64 is exact.
fn unit_interval(random: u64) -> f64 {
    const MANTISSA_BITS: u32 = 53;
    (random >> (u64::BITS - MANTISSA_BITS)) as f64 / (1u64 << MANTISSA_BITS) as f64
}

/// xorshift64*. Its only purpose is to spread retry delays; it is not
/// suitable for work that needs real randomness. It avoids one more
/// dependency in a binary that must stay small for an armv6 Pi Zero.
fn next_random(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// A per-process seed, from the same source `HashMap` uses for its hash keys.
fn random_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher, RandomState};
    RandomState::new().build_hasher().finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    const INITIAL: Duration = Duration::from_secs(5);
    const MAX: Duration = Duration::from_secs(60);
    const HEALTHY: Duration = Duration::from_secs(120);
    /// An attempt that failed at once: the adapter-missing case.
    const IMMEDIATE: Duration = Duration::ZERO;

    /// Jitter is off, so the schedule is exact and the assertions test the
    /// backoff, not the PRNG.
    fn backoff() -> Backoff {
        Backoff::with_seed(INITIAL, MAX, HEALTHY, 0.0, 1)
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_cancellation_when_waiting_then_it_finishes_after_the_delay() {
        let mut strategy = backoff();
        let start = tokio::time::Instant::now();

        let result = strategy.wait(IMMEDIATE, CancellationToken::new()).await;

        assert_eq!(result, Wait::Finished);
        assert_eq!(start.elapsed(), INITIAL);
    }

    #[tokio::test(start_paused = true)]
    async fn given_cancellation_during_wait_then_it_returns_cancelled_early() {
        let mut strategy = Backoff::with_seed(MAX, MAX, HEALTHY, 0.0, 1);
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let start = tokio::time::Instant::now();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token_clone.cancel();
        });

        let result = strategy.wait(IMMEDIATE, token).await;

        assert_eq!(result, Wait::Cancelled);
        assert!(start.elapsed() < MAX);
    }

    #[tokio::test(start_paused = true)]
    async fn given_repeated_failures_when_waiting_then_each_delay_doubles_up_to_the_cap() {
        // An absent adapter must not cost a log line every five seconds
        // forever.
        let mut strategy = backoff();
        let expected = [5, 10, 20, 40, 60, 60, 60].map(Duration::from_secs);

        for (attempt, want) in expected.into_iter().enumerate() {
            let start = tokio::time::Instant::now();
            strategy.wait(IMMEDIATE, CancellationToken::new()).await;
            assert_eq!(start.elapsed(), want, "attempt {}", attempt + 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_backed_off_strategy_when_an_attempt_ran_healthily_then_the_delay_starts_over()
    {
        // A bridge that ran for hours before a failure must retry promptly,
        // and must not inherit the cap from a fault sequence days earlier.
        let mut strategy = backoff();
        for _ in 0..5 {
            strategy.wait(IMMEDIATE, CancellationToken::new()).await;
        }

        let start = tokio::time::Instant::now();
        strategy.wait(HEALTHY, CancellationToken::new()).await;
        assert_eq!(start.elapsed(), INITIAL);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_attempt_just_short_of_healthy_when_it_fails_then_the_backoff_is_kept() {
        let mut strategy = backoff();
        strategy.wait(IMMEDIATE, CancellationToken::new()).await;

        let start = tokio::time::Instant::now();
        strategy
            .wait(HEALTHY - Duration::from_secs(1), CancellationToken::new())
            .await;
        assert_eq!(start.elapsed(), INITIAL * 2);
    }

    #[test]
    fn given_jitter_when_applied_then_it_only_ever_shortens_the_delay() {
        // Because jitter never lengthens a delay, the cap stays a real bound,
        // not an approximate one.
        let mut rng = 12345;
        for _ in 0..1000 {
            let jittered = apply_jitter(MAX, DEFAULT_JITTER, next_random(&mut rng));
            assert!(jittered <= MAX, "{jittered:?} exceeded the cap");
            assert!(
                jittered >= MAX.mul_f64(1.0 - DEFAULT_JITTER),
                "{jittered:?} took off more than {DEFAULT_JITTER} of the delay"
            );
        }
    }

    #[test]
    fn given_jitter_when_it_is_disabled_then_the_delay_is_exact() {
        assert_eq!(apply_jitter(INITIAL, 0.0, u64::MAX), INITIAL);
    }

    #[test]
    fn given_a_run_of_delays_when_jittered_then_they_actually_differ() {
        // Guards the guard: a jitter that always returns the base passes the
        // bounds test above and spreads nothing.
        let mut rng = 12345;
        let delays: Vec<Duration> = (0..10)
            .map(|_| apply_jitter(MAX, DEFAULT_JITTER, next_random(&mut rng)))
            .collect();
        assert!(
            delays.windows(2).any(|pair| pair[0] != pair[1]),
            "jitter produced identical delays: {delays:?}"
        );
    }

    #[test]
    fn given_any_random_when_mapped_to_the_unit_interval_then_it_is_below_one() {
        assert_eq!(unit_interval(0), 0.0);
        assert!(unit_interval(u64::MAX) < 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_jitter_is_enabled_when_waiting_then_the_delay_stays_within_bounds() {
        let mut strategy = Backoff::with_seed(INITIAL, MAX, HEALTHY, DEFAULT_JITTER, 42);
        let start = tokio::time::Instant::now();

        strategy.wait(IMMEDIATE, CancellationToken::new()).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed <= INITIAL,
            "{elapsed:?} exceeded the scheduled delay"
        );
        assert!(
            elapsed >= INITIAL.mul_f64(1.0 - DEFAULT_JITTER),
            "{elapsed:?}"
        );
    }
}
