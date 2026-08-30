//! The scanner abstraction: what a platform's BLE stack must yield for the
//! bridge to work. Linux uses bluer; other platforms use btleplug.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use futures_util::stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::BoxError;

/// One received advertisement, reduced to the only part that this bridge
/// reads. This is not `bluer::adv::Advertisement`, which the bridge
/// *broadcasts*.
#[derive(Debug, Clone)]
pub struct ReceivedAdvertisement {
    /// Address or id of the sender. For logging only — nothing branches on it.
    pub device: String,
    pub manufacturer_data: HashMap<u16, Vec<u8>>,
}

#[derive(Debug)]
pub enum ScanEvent {
    Advertisement(ReceivedAdvertisement),
    /// The scan cannot continue. The caller releases its resources, and
    /// `bridge_loop` decides whether to retry.
    Error(BoxError),
}

pub type ScanStream = Pin<Box<dyn Stream<Item = ScanEvent> + Send>>;

/// Something that yields BLE advertisements.
///
/// The boundary is deliberately *raw manufacturer data*, not parsed
/// `KeiserStats`. The Keiser id match, the parse, the bike-id filter, and the
/// logging stay above this trait. If each platform implementation held that
/// logic, CI could compile only one half of the duplicate. Above the trait,
/// tests that run on every platform cover all of it.
pub trait BleScanner {
    /// A desugared RPITIT instead of `async fn` makes the `Send` bound on the
    /// returned future explicit. The stream ends to signal cancellation; no
    /// dedicated event exists.
    fn scan(
        &self,
        cancel_token: CancellationToken,
    ) -> impl Future<Output = Result<ScanStream, BoxError>> + Send;
}

/// Selects the advertisements that get a trace line: the first few, which
/// show that a scan starts, then one in every hundred. This is the 2 Hz hot
/// path, and every bike in range contributes.
pub fn is_sampled_for_trace(count: u64) -> bool {
    const TRACE_FIRST: u64 = 10;
    const TRACE_EVERY: u64 = 100;
    count < TRACE_FIRST || count.is_multiple_of(TRACE_EVERY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_advertisement_counts_when_sampled_then_the_first_few_and_every_hundredth_are_traced() {
        assert!(is_sampled_for_trace(0));
        assert!(is_sampled_for_trace(9));
        assert!(!is_sampled_for_trace(10));
        assert!(!is_sampled_for_trace(99));
        assert!(is_sampled_for_trace(100));
        assert!(!is_sampled_for_trace(101));
        assert!(is_sampled_for_trace(7200), "one an hour at 2 Hz");
    }
}
