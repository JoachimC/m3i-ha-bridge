//! The scanner abstraction: what a platform's BLE stack has to yield for the
//! bridge to work — bluer on Linux, btleplug elsewhere.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use futures_util::stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::BoxError;

/// One received advertisement, reduced to the only part this bridge reads.
/// Not to be confused with `bluer::adv::Advertisement`, which is what the
/// bridge *broadcasts*.
#[derive(Debug, Clone)]
pub struct ReceivedAdvertisement {
    /// Address or id of the sender. For logging only — nothing branches on it.
    pub device: String,
    pub manufacturer_data: HashMap<u16, Vec<u8>>,
}

#[derive(Debug)]
pub enum ScanEvent {
    Advertisement(ReceivedAdvertisement),
    /// The scan cannot continue. The caller tears down and `bridge_loop`
    /// decides whether to retry.
    Error(BoxError),
}

pub type ScanStream = Pin<Box<dyn Stream<Item = ScanEvent> + Send>>;

/// Something that yields BLE advertisements.
///
/// The boundary is deliberately *raw manufacturer data* rather than parsed
/// `KeiserStats`. Pushing the Keiser id match, the parse, the bike-id filter
/// and the logging down into the platform implementations would duplicate the
/// only interesting logic in the crate across a `cfg` split that CI can compile
/// just one half of. Kept above this trait, all of it is covered by tests that
/// run everywhere.
pub trait BleScanner {
    /// Written as a desugared RPITIT rather than `async fn` so the `Send` bound
    /// on the returned future is explicit. Cancellation is signalled by ending
    /// the stream; there is no dedicated event.
    fn scan(
        &self,
        cancel_token: CancellationToken,
    ) -> impl Future<Output = Result<ScanStream, BoxError>> + Send;
}

/// Which advertisements are worth a trace line: the first few, so a scan that
/// starts can be seen to, then one in every hundred. This is the 2 Hz hot
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
