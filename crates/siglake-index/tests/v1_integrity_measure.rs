//! #4991: what a stored v1 inverted-index blob's integrity rests on, and what
//! a whole-blob checksum would cost.
//!
//! A v1 blob reaches a reader in one of two stored forms, and they are not
//! equally exposed:
//!
//! - **footer KV**: lowercase hex in the Parquet footer's `key_value_metadata`
//!   (`InvertedIndex::to_hex`). Parquet checksums data pages, not footer
//!   metadata, so the stored bytes carry nothing but the hex alphabet.
//! - **Puffin sidecar**: a blob compressed with `CompressionCodec::Zstd`, whose
//!   encoder sets `include_checksum(true)`
//!   (`third_party/iceberg/src/compression.rs`). The frame is what a corrupt
//!   byte hits first.
//!
//! The default-run tests here assert the qualitative claim for each form; the
//! two `#[ignore]`d reports print the rates and the price the design document
//! quotes:
//!
//! ```
//! cargo test -p siglake-index --release --test v1_integrity_measure \
//!   report_stored_byte_corruption_by_path -- --ignored --nocapture
//! cargo test -p siglake-index --release --test v1_integrity_measure \
//!   report_whole_blob_checksum_cost -- --ignored --nocapture
//! ```
//!
//! Sized by `SIGLAKE_V1_INTEGRITY_{ROWS,SAMPLE,RUNS}`.

use std::time::{Duration, Instant};

use siglake_index::InvertedIndex;

/// One measurement row's text, the same shape `segmented_measure.rs` uses: one
/// token unique to the row, so the dictionary grows with the rows, plus the
/// shared `queen` term the probes look up.
fn raw(row: usize) -> String {
    let queen = if row.is_multiple_of(50) { " queen" } else { "" };
    let checkout = if row.is_multiple_of(20) {
        " checkout"
    } else {
        ""
    };
    format!(
        "service-{} status {}{queen}{checkout} row-{row:06}",
        row % 20,
        200 + row % 5
    )
}

fn knob(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Rows in the fixture the default-run assertions sweep. Small on purpose: the
/// sweep is exhaustive and these run in a debug build on every `cargo test`,
/// while the two reports default to a thousand rows and are run on request.
const ASSERTION_ROWS: usize = 250;

fn fixture(rows: usize) -> InvertedIndex {
    let rows: Vec<String> = (0..rows).map(raw).collect();
    InvertedIndex::from_rows(rows.iter().map(String::as_str))
}

/// The probe term, and the postings a sound blob answers with.
const PROBE: &str = "queen";

/// A Puffin blob's stored bytes, framed as the fork's `CompressionCodec::Zstd`
/// frames them: level 3, content size pledged, content checksum on.
/// `include_checksum(false)` is the control — what the frame catches on its own,
/// without the four checksum bytes
/// (`third_party/iceberg/src/compression.rs` sets them).
fn zstd_frame_with(bytes: &[u8], checksum: bool) -> Vec<u8> {
    let mut encoder = zstd::stream::Encoder::new(Vec::<u8>::new(), 3).expect("zstd encoder");
    encoder.include_checksum(checksum).expect("checksum choice");
    encoder
        .set_pledged_src_size(Some(bytes.len() as u64))
        .expect("pledged size");
    std::io::copy(&mut &bytes[..], &mut encoder).expect("compress");
    encoder.finish().expect("finish frame")
}

fn zstd_frame(bytes: &[u8]) -> Vec<u8> {
    zstd_frame_with(bytes, true)
}

/// What one corruption did to the answer, in the order the two layers fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// The stored form never became an index: hex alphabet, Zstd frame, or
    /// `from_bytes`. The reader falls back to an exact scan — slower, right.
    Refused,
    /// Decoded, and the reader's second layer catches it: `n_rows` no longer
    /// matches the Parquet file's row count, so `index_covers_file` refuses it
    /// warm or cold (#4558). Also an exact scan.
    RowDomainRefused,
    /// Decoded to the same index. A byte the format does not read, or a hex
    /// digit's case.
    Identical,
    /// Decoded to a different index that still names every `(term, row)` the
    /// sound blob named. Extra rows and terms are harmless: the exact
    /// predicate runs above the scan.
    DiffersSuperset,
    /// Decoded, passed the row-domain check, and lost a `(term, row)` the
    /// sound blob named: a match the scan skips before decode. This is the
    /// residual #4558 measured and this card prices.
    LostRows,
}

/// A corruption's disposition, and whether it cost the probe term rows — the
/// per-query view #4560's sweep reported, which is a much smaller share than
/// the whole-dictionary one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outcome {
    disposition: Disposition,
    probe_lost: bool,
}

/// The first `(term, row)` in `sound` that `corrupt` no longer names, if any.
/// Both dictionaries are term-ascending, so this is a merge walk; probing one
/// term would miss a flip inside another term's postings, and the query path
/// looks up whatever the predicate asks for. A term whose *characters* were
/// flipped counts here: `matching_rows_all` reads an absent term as a
/// definitive "no rows match" and skips the whole file.
fn first_lost_posting<'a>(
    sound: &'a InvertedIndex,
    corrupt: &InvertedIndex,
) -> Option<(&'a str, u32)> {
    let mut theirs = corrupt.terms().peekable();
    for (term, rows) in sound.terms() {
        while theirs.peek().is_some_and(|(other, _)| *other < term) {
            theirs.next();
        }
        match theirs.peek() {
            Some((other, got)) if *other == term => {
                if let Some(row) = rows.iter().find(|row| got.binary_search(row).is_err()) {
                    return Some((term, *row));
                }
            }
            _ => return Some((term, rows[0])),
        }
    }
    None
}

fn classify(candidate: Option<InvertedIndex>, sound: &InvertedIndex) -> Outcome {
    let bare = |disposition| Outcome {
        disposition,
        probe_lost: false,
    };
    let Some(index) = candidate else {
        return bare(Disposition::Refused);
    };
    if index == *sound {
        return bare(Disposition::Identical);
    }
    // The fixture stands in for a file whose Parquet row count is the sound
    // index's `n_rows`, so this is the reader's `index_covers_file`.
    if index.n_rows() != sound.n_rows() {
        return bare(Disposition::RowDomainRefused);
    }
    let truth = sound.postings(PROBE).expect("probe term present");
    let probe_lost = match index.postings(PROBE) {
        None => true,
        Some(got) => truth.iter().any(|row| got.binary_search(row).is_err()),
    };
    Outcome {
        disposition: match first_lost_posting(sound, &index) {
            None => Disposition::DiffersSuperset,
            Some(_) => Disposition::LostRows,
        },
        probe_lost,
    }
}

fn classify_raw(bytes: &[u8], sound: &InvertedIndex) -> Outcome {
    classify(InvertedIndex::from_bytes(bytes), sound)
}

fn classify_hex(hex: &str, sound: &InvertedIndex) -> Outcome {
    classify(InvertedIndex::from_hex(hex), sound)
}

fn classify_frame(frame: &[u8], sound: &InvertedIndex) -> Outcome {
    match zstd::stream::decode_all(frame) {
        Err(_) => classify(None, sound),
        Ok(bytes) => classify_raw(&bytes, sound),
    }
}

/// Every single-bit flip of `len` bytes, visited in a fixed order, at most
/// `sample` of them evenly spaced. `sample == 0` means all of them.
fn flips(len: usize, sample: usize) -> impl Iterator<Item = (usize, u32)> {
    let total = len * 8;
    let step = if sample == 0 || sample >= total {
        1
    } else {
        total / sample
    };
    (0..total)
        .step_by(step)
        .map(|flip| (flip / 8, (flip % 8) as u32))
}

/// The footer-KV path has no cover: a flip inside a posting delta that leaves
/// the ordinals ascending and inside `n_rows` decodes, prunes, and drops a row
/// the query should have returned. This is the exposure priced in
/// `docs/LIMITATIONS.md`; the search is here so the fixture cannot rot into a
/// blob where no such flip exists.
#[test]
fn a_single_bit_flip_in_a_v1_blob_can_silently_drop_a_match() {
    let sound = fixture(knob("SIGLAKE_V1_INTEGRITY_ROWS", ASSERTION_ROWS));
    assert!(sound.postings(PROBE).is_some(), "probe term present");
    let blob = sound.to_bytes();

    let lost = flips(blob.len(), 0).find(|(byte, bit)| {
        let mut corrupt = blob.clone();
        corrupt[*byte] ^= 1 << bit;
        classify_raw(&corrupt, &sound).disposition == Disposition::LostRows
    });
    let (byte, bit) = lost.expect("some single-bit flip of a v1 blob loses a match silently");

    // And the per-query form of the same exposure: a flip that costs the rows
    // of the term a query actually asks for. Rarer than a loss anywhere in the
    // dictionary — `report_stored_byte_corruption_by_path` prints both rates.
    assert!(
        flips(blob.len(), 0).any(|(byte, bit)| {
            let mut corrupt = blob.clone();
            corrupt[byte] ^= 1 << bit;
            let outcome = classify_raw(&corrupt, &sound);
            outcome.disposition == Disposition::LostRows && outcome.probe_lost
        }),
        "some flip costs the probe term rows while the blob still decodes"
    );

    // The same flip in the stored footer form. Hex is an encoding, not a
    // check: it carries the flip through as a nibble change.
    let mut corrupt = blob.clone();
    corrupt[byte] ^= 1 << bit;
    let hex: String = corrupt.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        classify_hex(&hex, &sound).disposition,
        Disposition::LostRows,
        "byte {byte} bit {bit}"
    );
}

/// The Puffin path's cover, on the shipped codec: a flip in the stored bytes
/// hits the Zstd frame, and the frame does not hand the decoder anything. The
/// disposition is an error out of `PuffinReader::blob`, which the scan
/// propagates — the query fails rather than answering short
/// (`inverted_index_integrity.rs` in siglake-storage holds that half).
#[test]
fn a_single_bit_flip_in_a_zstd_framed_blob_never_reaches_the_decoder() {
    let sound = fixture(knob("SIGLAKE_V1_INTEGRITY_ROWS", ASSERTION_ROWS));
    let frame = zstd_frame(&sound.to_bytes());
    assert_eq!(
        zstd::stream::decode_all(&frame[..]).expect("sound frame decompresses"),
        sound.to_bytes()
    );

    let sample = knob("SIGLAKE_V1_INTEGRITY_SAMPLE", 2_000);
    let mut refused = 0usize;
    let mut identical = 0usize;
    let mut reached_decoder = Vec::new();
    for (byte, bit) in flips(frame.len(), sample) {
        let mut corrupt = frame.clone();
        corrupt[byte] ^= 1 << bit;
        match classify_frame(&corrupt, &sound).disposition {
            Disposition::Refused => refused += 1,
            Disposition::Identical => identical += 1,
            other => reached_decoder.push((byte, bit, other)),
        }
    }
    assert!(refused > 0, "the sample has to exercise the frame");
    assert!(
        reached_decoder.is_empty(),
        "a corrupt frame produced a different index: {reached_decoder:?}"
    );
    // `Identical` here would be a flip the frame absorbed (a padding or
    // header bit that does not change the payload); it is still not a wrong
    // answer. Recorded so the report's columns are explained.
    assert_eq!(
        refused + identical,
        flips(frame.len(), sample).count(),
        "every flip is refused or absorbed"
    );
}

/// Hex doubles the stored bytes and refuses a share of flips by accident — the
/// four high bits of an ASCII hex digit mostly leave the alphabet. That is not
/// integrity: the flips that stay in the alphabet are exactly nibble
/// corruptions of the blob, and they carry through to the decoder.
#[test]
fn the_hex_footer_form_refuses_flips_by_alphabet_not_by_checking() {
    let sound = fixture(knob("SIGLAKE_V1_INTEGRITY_ROWS", ASSERTION_ROWS));
    let hex = sound.to_hex();
    let bytes = hex.as_bytes();

    let sample = knob("SIGLAKE_V1_INTEGRITY_SAMPLE", 2_000);
    let mut refused = 0usize;
    let mut decoded = 0usize;
    for (byte, bit) in flips(bytes.len(), sample) {
        let mut corrupt = bytes.to_vec();
        corrupt[byte] ^= 1 << bit;
        let Ok(corrupt) = String::from_utf8(corrupt) else {
            refused += 1;
            continue;
        };
        match classify_hex(&corrupt, &sound).disposition {
            Disposition::Refused => refused += 1,
            _ => decoded += 1,
        }
    }
    assert!(
        decoded > 0,
        "some flips of the hex form reach the decoder; {refused} refused"
    );
    assert!(
        refused > decoded,
        "most flips leave the hex alphabet: {refused} refused, {decoded} decoded"
    );
}

/// Compatibility fixtures for the two placements a checksum could take. Both
/// are refused by the shipped decoder, which is the fallback a 0.1.x reader
/// gives a blob it cannot read: no index, exact scan, right answer.
#[test]
fn the_shipped_decoder_refuses_both_checksum_placements() {
    let sound = fixture(200);
    let blob = sound.to_bytes();
    assert_eq!(
        InvertedIndex::from_bytes(&blob).as_ref(),
        Some(&sound),
        "the fixture round-trips"
    );

    // Placement 1: append the CRC and keep version 1. `from_bytes` consumes a
    // well-formed blob exactly (crates/siglake-index/src/lib.rs, the trailing
    // payload check), so an appended checksum is refused — a v1 reader would
    // stop pruning with every blob a v2 writer wrote.
    let mut appended = blob.clone();
    appended.extend_from_slice(&crc32fast::hash(&blob).to_le_bytes());
    assert!(
        InvertedIndex::from_bytes(&appended).is_none(),
        "a trailing checksum is refused, not ignored"
    );

    // Placement 2: bump the version byte. Refused at the version check, which
    // is the same disposition and says so explicitly.
    let mut versioned = appended.clone();
    versioned[4] = 2;
    assert!(
        InvertedIndex::from_bytes(&versioned).is_none(),
        "a v2 version byte is refused"
    );

    // And the direction that has to keep working either way: 0.1.x blobs on
    // disk stay readable, because the version byte is what selects the layout.
    assert_eq!(InvertedIndex::from_bytes(&blob).as_ref(), Some(&sound));
}

/// Exhaustive single-bit sweep of all three stored forms, as a run-on-request
/// report. The default-run tests above assert the qualitative claim per form;
/// this prints the rates `docs/DESIGN_inverted_index.md` quotes.
#[test]
#[ignore]
fn report_stored_byte_corruption_by_path() {
    let sound = fixture(knob("SIGLAKE_V1_INTEGRITY_ROWS", 1_000));
    assert!(sound.postings(PROBE).is_some(), "probe term present");
    let blob = sound.to_bytes();
    let hex = sound.to_hex();
    let frame = zstd_frame(&blob);
    let sample = knob("SIGLAKE_V1_INTEGRITY_SAMPLE", 0);

    let tally = |len: usize, classify: &dyn Fn(usize, u32) -> Outcome| {
        let mut counts = [0usize; 6];
        let mut seen = 0usize;
        for (byte, bit) in flips(len, sample) {
            seen += 1;
            let outcome = classify(byte, bit);
            counts[match outcome.disposition {
                Disposition::Refused => 0,
                Disposition::RowDomainRefused => 1,
                Disposition::Identical => 2,
                Disposition::DiffersSuperset => 3,
                Disposition::LostRows => 4,
            }] += 1;
            if outcome.probe_lost {
                counts[5] += 1;
            }
        }
        (seen, counts)
    };

    let payload = tally(blob.len(), &|byte, bit| {
        let mut corrupt = blob.clone();
        corrupt[byte] ^= 1 << bit;
        classify_raw(&corrupt, &sound)
    });
    let footer = tally(hex.len(), &|byte, bit| {
        let mut corrupt = hex.clone().into_bytes();
        corrupt[byte] ^= 1 << bit;
        match String::from_utf8(corrupt) {
            Err(_) => classify(None, &sound),
            Ok(corrupt) => classify_hex(&corrupt, &sound),
        }
    });
    let sidecar = tally(frame.len(), &|byte, bit| {
        let mut corrupt = frame.clone();
        corrupt[byte] ^= 1 << bit;
        classify_frame(&corrupt, &sound)
    });
    // Control for the row above: the same frame without the four checksum
    // bytes, which is how much of the cover is the codec's framing rather than
    // `include_checksum(true)`.
    let plain = zstd_frame_with(&blob, false);
    let unchecked = tally(plain.len(), &|byte, bit| {
        let mut corrupt = plain.clone();
        corrupt[byte] ^= 1 << bit;
        classify_frame(&corrupt, &sound)
    });

    println!(
        "\nrows {} | terms {} | v1 payload {} B | hex footer {} B | zstd-3 frame {} B\n",
        sound.n_rows(),
        sound.n_terms(),
        blob.len(),
        hex.len(),
        frame.len()
    );
    println!(
        "| stored form | bytes | flips | refused decoding | refused row domain | absorbed | wrong, superset | lost a match | lost the probe term's rows |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (label, len, (seen, counts)) in [
        ("v1 payload, uncompressed", blob.len(), payload),
        ("footer KV, hex as stored", hex.len(), footer),
        (
            "Puffin sidecar, zstd-3 frame as stored",
            frame.len(),
            sidecar,
        ),
        (
            "control: zstd-3 frame, content checksum off",
            plain.len(),
            unchecked,
        ),
    ] {
        println!(
            "| {label} | {len} | {seen} | {} | {} | {} | {} | {} | {} |",
            counts[0], counts[1], counts[2], counts[3], counts[4], counts[5]
        );
    }
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

/// What one CRC-32 over the whole blob would cost: bytes at rest in each stored
/// form, and CPU at encode and decode, against the encode and decode the format
/// already pays. Run on request; the numbers land in
/// `docs/DESIGN_inverted_index.md`.
#[test]
#[ignore]
fn report_whole_blob_checksum_cost() {
    let rows = knob("SIGLAKE_V1_INTEGRITY_ROWS", 200_000);
    let runs = knob("SIGLAKE_V1_INTEGRITY_RUNS", 9).max(1);
    let sound = fixture(rows);
    let blob = sound.to_bytes();
    let hex = sound.to_hex();

    let mut encode = Vec::new();
    let mut decode = Vec::new();
    let mut crc = Vec::new();
    let mut hex_encode = Vec::new();
    for _ in 0..runs {
        let started = Instant::now();
        let bytes = sound.to_bytes();
        encode.push(started.elapsed());
        let started = Instant::now();
        let back = InvertedIndex::from_bytes(&bytes).expect("round-trips");
        decode.push(started.elapsed());
        assert_eq!(back.n_rows(), sound.n_rows());
        let started = Instant::now();
        let sum = crc32fast::hash(&bytes);
        crc.push(started.elapsed());
        assert_ne!(sum, 0);
        let started = Instant::now();
        let text = sound.to_hex();
        hex_encode.push(started.elapsed());
        assert_eq!(text.len(), hex.len());
    }
    let (encode, decode, crc, hex_encode) = (
        median(encode),
        median(decode),
        median(crc),
        median(hex_encode),
    );
    let mib = blob.len() as f64 / (1024.0 * 1024.0);

    println!(
        "\nrows {} | terms {} | v1 payload {} B ({:.2} MiB) | hex {} B\n",
        sound.n_rows(),
        sound.n_terms(),
        blob.len(),
        mib,
        hex.len()
    );
    println!("| cost | value | against |");
    println!("|---|---:|---|");
    println!(
        "| checksum field | 4 B | {:.6}% of the {} B payload |",
        400.0 / blob.len() as f64,
        blob.len()
    );
    println!(
        "| footer KV, hex | 8 chars | {:.6}% of the {} B footer value |",
        800.0 / hex.len() as f64,
        hex.len()
    );
    println!(
        "| CRC-32 over the blob | {:.3} ms | {:.0} MiB/s |",
        crc.as_secs_f64() * 1e3,
        mib / crc.as_secs_f64()
    );
    println!(
        "| against encode (`to_bytes`) | {:.1} ms | +{:.2}% |",
        encode.as_secs_f64() * 1e3,
        100.0 * crc.as_secs_f64() / encode.as_secs_f64()
    );
    println!(
        "| against decode (`from_bytes`) | {:.1} ms | +{:.2}% |",
        decode.as_secs_f64() * 1e3,
        100.0 * crc.as_secs_f64() / decode.as_secs_f64()
    );
    println!(
        "| against hex encode (`to_hex`) | {:.1} ms | +{:.2}% |",
        hex_encode.as_secs_f64() * 1e3,
        100.0 * crc.as_secs_f64() / hex_encode.as_secs_f64()
    );
}
