use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, PartialEq)]
pub enum BetweenRetriesResult {
    Finished,
    Cancelled,
}

pub trait BetweenRetriesStrategy {
    fn wait(
        &self,
        cancel_token: CancellationToken,
    ) -> impl Future<Output = BetweenRetriesResult> + Send;

    /// Called when an attempt ran long enough to count as healthy, so a
    /// backoff built up by a spell of failures does not outlive it.
    ///
    /// Defaulted to nothing, because a strategy with no state has nothing to
    /// forget.
    fn reset(&self) {}
}

/// Exponential backoff with jitter.
///
/// A fixed short delay is right while a failure looks transient, but the
/// adapter-missing case never resolves on its own: the bridge logs an error,
/// waits, fails identically, and repeats forever. At five seconds that is
/// seventeen thousand identical log lines a day, on a box whose journal lives
/// on an SD card. Doubling to a cap keeps the first retry fast — which is what
/// matters for a transient fault — while making a permanent one cheap to leave
/// running until someone can look at it.
pub struct ExponentialBackoff {
    initial: Duration,
    max: Duration,
    /// Fraction of each delay that jitter may remove, e.g. `0.1` for "up to
    /// 10% shorter".
    jitter: f64,
    state: Mutex<BackoffState>,
}

struct BackoffState {
    /// Delay for the next `wait`, before jitter.
    next: Duration,
    rng: u64,
}

/// Up to 10% off each delay. With a single bridge there is no thundering herd
/// to spread, but it keeps the retries from locking in step with anything else
/// that restarts on the same timer — bluetoothd, most obviously.
const DEFAULT_JITTER: f64 = 0.1;

impl ExponentialBackoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self::with_seed(initial, max, DEFAULT_JITTER, random_seed())
    }

    /// Construction with an explicit jitter fraction and PRNG seed, so tests
    /// are deterministic. A zero seed would make the generator produce nothing
    /// but zeroes, so it is nudged odd.
    pub fn with_seed(initial: Duration, max: Duration, jitter: f64, seed: u64) -> Self {
        Self {
            initial,
            max,
            jitter,
            state: Mutex::new(BackoffState {
                next: initial,
                rng: seed | 1,
            }),
        }
    }

    /// Takes the next delay and advances the schedule.
    ///
    /// Deliberately not `async`: the lock must be released before anything is
    /// awaited.
    fn take_delay(&self) -> Duration {
        let mut state = self.state.lock().expect("backoff state poisoned");
        let base = state.next;
        state.next = base.saturating_mul(2).min(self.max);
        let random = next_random(&mut state.rng);
        drop(state);
        apply_jitter(base, self.jitter, random)
    }
}

impl BetweenRetriesStrategy for ExponentialBackoff {
    async fn wait(&self, cancel_token: CancellationToken) -> BetweenRetriesResult {
        let delay = self.take_delay();
        tokio::select! {
            _ = cancel_token.cancelled() => BetweenRetriesResult::Cancelled,
            _ = tokio::time::sleep(delay) => BetweenRetriesResult::Finished,
        }
    }

    fn reset(&self) {
        self.state.lock().expect("backoff state poisoned").next = self.initial;
    }
}

/// Shortens `base` by up to `fraction` of itself.
///
/// Jitter only ever subtracts, so a jittered delay can never exceed the cap or
/// the delay actually scheduled — worth keeping true, because "at most `max`"
/// is easier to reason about than "roughly `max`".
fn apply_jitter(base: Duration, fraction: f64, random: u64) -> Duration {
    if fraction <= 0.0 {
        return base;
    }
    // The top 53 bits give a uniform value in [0, 1) with no rounding surprises.
    let unit = (random >> 11) as f64 / (1u64 << 53) as f64;
    base.mul_f64(1.0 - fraction.clamp(0.0, 1.0) * unit)
}

/// xorshift64*. Spreading retry delays is all this is for; it is not suitable
/// for anything that cares about randomness, but it avoids a dependency on a
/// binary that is being trimmed for an armv6 Pi Zero.
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

    /// Jitter off, so the schedule is exact and the assertions are about
    /// backoff rather than about the PRNG.
    fn backoff() -> ExponentialBackoff {
        ExponentialBackoff::with_seed(INITIAL, MAX, 0.0, 1)
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_cancellation_when_waiting_then_it_finishes_after_the_delay() {
        let strategy = backoff();
        let start = tokio::time::Instant::now();

        let result = strategy.wait(CancellationToken::new()).await;

        assert_eq!(result, BetweenRetriesResult::Finished);
        assert_eq!(start.elapsed(), INITIAL);
    }

    #[tokio::test(start_paused = true)]
    async fn given_cancellation_during_wait_then_it_returns_cancelled_early() {
        let strategy = ExponentialBackoff::with_seed(MAX, MAX, 0.0, 1);
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let start = tokio::time::Instant::now();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token_clone.cancel();
        });

        let result = strategy.wait(token).await;

        assert_eq!(result, BetweenRetriesResult::Cancelled);
        assert!(start.elapsed() < MAX);
    }

    #[tokio::test(start_paused = true)]
    async fn given_repeated_failures_when_waiting_then_each_delay_doubles_up_to_the_cap() {
        // The point of the issue: an adapter that is simply absent must not
        // keep costing a log line every five seconds forever.
        let strategy = backoff();
        let expected = [5, 10, 20, 40, 60, 60, 60].map(Duration::from_secs);

        for (attempt, want) in expected.into_iter().enumerate() {
            let start = tokio::time::Instant::now();
            strategy.wait(CancellationToken::new()).await;
            assert_eq!(start.elapsed(), want, "attempt {}", attempt + 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_backed_off_strategy_when_it_is_reset_then_the_next_delay_is_the_initial_one() {
        // A bridge that ran healthily for hours before failing should retry
        // promptly, not inherit the cap from a bad spell days earlier.
        let strategy = backoff();
        for _ in 0..5 {
            strategy.wait(CancellationToken::new()).await;
        }

        strategy.reset();

        let start = tokio::time::Instant::now();
        strategy.wait(CancellationToken::new()).await;
        assert_eq!(start.elapsed(), INITIAL);
    }

    #[test]
    fn given_jitter_when_applied_then_it_only_ever_shortens_the_delay() {
        // Never lengthening is what keeps the cap a real bound rather than an
        // approximate one.
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
        // Guards the guard: a jitter that always returned the base would pass
        // the bounds test above while spreading nothing.
        let mut rng = 12345;
        let delays: Vec<Duration> = (0..10)
            .map(|_| apply_jitter(MAX, DEFAULT_JITTER, next_random(&mut rng)))
            .collect();
        assert!(
            delays.windows(2).any(|pair| pair[0] != pair[1]),
            "jitter produced identical delays: {delays:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_jitter_is_enabled_when_waiting_then_the_delay_stays_within_bounds() {
        let strategy = ExponentialBackoff::with_seed(INITIAL, MAX, DEFAULT_JITTER, 42);
        let start = tokio::time::Instant::now();

        strategy.wait(CancellationToken::new()).await;

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
