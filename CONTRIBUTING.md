# Contributing

Thank you for your interest. One person maintains this project. The process
is small, but these rules keep the history easy to read.

## Before you start

1. Open an issue first. Do this for every change that is larger than a typo.
2. Describe the behaviour that you see.
3. For a Bluetooth problem, attach the output of `sudo btmon`. Most bugs in
   this project are in the data that the radio received.
4. Read the open issues. Some known tasks are already there.

## Workflow

1. Make one branch for one issue. Start the branch from the current `main`.
2. Name the branch `issue-N-short-description`.
3. Open one pull request for the branch. Put `Closes #N` in the pull request
   body. The issue then closes when the pull request merges.
4. Keep the pull request small. Put format-only changes and rename-only
   changes in their own pull requests.
5. In the pull request body, say if you tested the change on real hardware.
   Tests on a development machine and in CI do not run the GATT server, the
   BlueZ scanner or the systemd unit. You can submit a change to those areas
   that you only compiled. But say so.

## Code

* Use Rust 1.88 or later. `Cargo.toml` sets `rust-version`, and the `msrv`
  CI job enforces it. The edition is 2024.
* Before you push, run these commands. CI runs the same commands.
  ```bash
  cargo fmt --check
  cargo clippy --all-targets -- -D warnings
  cargo test
  cargo build --release
  ```
* The modules under `cfg(target_os = "linux")` (`gatt_server.rs`,
  `scan_bluer.rs`) do not compile on macOS or Windows. On those systems,
  `cargo test` skips them and shows no warning. To build and test those
  modules, use the devcontainer in `.devcontainer/`, or a different Linux
  container.
* Keep `keiser.rs`, `gatt_codec.rs` and `stats.rs` free of Bluetooth-stack
  dependencies. This lets them build and test on all platforms.
* Write comments that say *why*, not *what*. The existing code records the
  measurement or the failure that caused a decision. Do the same when you add
  a decision that is not obvious.

## Tests

* Name each test `given_<state>_when_<action>_then_<outcome>`. The name must
  state the defect that the test prevents. Example:
  `given_imperial_flag_when_parsed_then_packet_is_rejected`.
* The protocol tests use real `btmon` captures. If you add a test vector,
  also add the raw capture to `doc/sample-data.md`.
* For a test that depends on time, use `#[tokio::test(start_paused = true)]`.
  Do not use real sleeps.

## Support and the Maintenance Fee

The project participates in the
[Open Source Maintenance Fee](https://opensourcemaintenancefee.org). See
`OSMFEULA.txt` and the README. Questions and issues from commercial users who
pay the fee get priority. All other questions and issues are welcome. The
maintainer answers them when time permits.

## Licensing

When you submit a contribution, you agree that it is licensed under
MIT OR Apache-2.0. This is the same licence as the project. See the License
section in `README.md`.
