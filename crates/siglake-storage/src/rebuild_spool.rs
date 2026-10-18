//! Private, bounded staging for production v1 index rebuilds. These limits
//! bound index state and scratch, not the Parquet decoder or allocator arenas.

use std::io::{Read, Write};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::Stream;
use tempfile::NamedTempFile;

pub(crate) const INDEX_MEMORY_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const INDEX_SCRATCH_BYTES: u64 = 1024 * 1024 * 1024;
pub(crate) const PUFFIN_SCRATCH_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const TRANSFER_BYTES: usize = 64 * 1024;

struct LimitedWriter<W> {
    inner: W,
    written: u64,
    limit: u64,
}

impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.limit.saturating_sub(self.written) {
            return Err(std::io::Error::other(
                "index compression scratch limit exceeded",
            ));
        }
        let count = self.inner.write(bytes)?;
        self.written += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Same Zstd level, content-size pledge and checksum as the ordinary Puffin
/// codec, with a bounded output spool instead of two whole in-memory blobs.
pub(crate) fn compress_v1(
    mut input: impl Read,
    raw_length: u64,
    level: u8,
) -> Result<NamedTempFile> {
    let mut output = NamedTempFile::new().context("create compressed index spool")?;
    {
        let sink = LimitedWriter {
            inner: output.as_file_mut(),
            written: 0,
            limit: INDEX_SCRATCH_BYTES,
        };
        let mut encoder = zstd::stream::Encoder::new(sink, i32::from(level))?;
        encoder.include_checksum(true)?;
        encoder.set_pledged_src_size(Some(raw_length))?;
        let mut transferred = 0u64;
        let mut buffer = [0u8; TRANSFER_BYTES];
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            transferred = transferred
                .checked_add(count as u64)
                .context("index spool length overflow")?;
            anyhow::ensure!(
                transferred <= raw_length,
                "index spool grew during compression"
            );
            encoder.write_all(&buffer[..count])?;
        }
        anyhow::ensure!(transferred == raw_length, "index spool was truncated");
        encoder.finish()?.flush()?;
    }
    Ok(output)
}

/// The file handle is owned by the stream; each yielded buffer is at most64KiB.
/// Keep the NamedTempFile owner alive until consumption ends (or is cancelled).
pub(crate) fn file_chunks(file: std::fs::File) -> impl Stream<Item = iceberg::Result<Bytes>> {
    futures::stream::try_unfold(tokio::fs::File::from_std(file), |mut file| async move {
        use tokio::io::AsyncReadExt;
        let mut buffer = vec![0u8; TRANSFER_BYTES];
        let count = file.read(&mut buffer).await.map_err(|error| {
            iceberg::Error::new(iceberg::ErrorKind::Unexpected, "read index spool")
                .with_source(error)
        })?;
        if count == 0 {
            return Ok(None);
        }
        buffer.truncate(count);
        Ok(Some((Bytes::from(buffer), file)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_compression_roundtrip_and_length_refusals() {
        let input = (0..200_000).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let output = compress_v1(input.as_slice(), input.len() as u64, 3).unwrap();
        assert_eq!(
            zstd::stream::decode_all(output.reopen().unwrap()).unwrap(),
            input
        );
        assert!(compress_v1(input.as_slice(), input.len() as u64 - 1, 3).is_err());
        assert!(compress_v1(input.as_slice(), input.len() as u64 + 1, 3).is_err());
    }

    #[test]
    fn compression_scratch_limit_refuses_before_extra_write() {
        let mut writer = LimitedWriter {
            inner: Vec::new(),
            written: 0,
            limit: 3,
        };
        writer.write_all(b"abc").unwrap();
        assert!(writer.write_all(b"d").is_err());
        assert_eq!(writer.inner, b"abc");
    }

    #[tokio::test]
    async fn spool_transfer_has_bounded_chunks_and_exact_bytes() {
        use futures::TryStreamExt;
        let input = vec![42u8; TRANSFER_BYTES * 3 + 7];
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&input).unwrap();
        let chunks = file_chunks(file.reopen().unwrap());
        futures::pin_mut!(chunks);
        let mut actual = Vec::new();
        while let Some(chunk) = chunks.try_next().await.unwrap() {
            assert!(chunk.len() <= TRANSFER_BYTES);
            actual.extend_from_slice(&chunk);
        }
        assert_eq!(actual, input);
    }

    fn blob(data: Vec<u8>) -> iceberg::puffin::Blob {
        iceberg::puffin::Blob::builder()
            .r#type("test-v1".to_string())
            .fields(vec![1])
            .snapshot_id(7)
            .sequence_number(3)
            .data(data)
            .properties(std::collections::HashMap::new())
            .build()
    }

    fn test_output(temp: &tempfile::TempDir, name: &str) -> iceberg::io::OutputFile {
        iceberg::io::FileIO::new_with_fs()
            .new_output(temp.path().join(name).to_str().unwrap())
            .unwrap()
    }

    #[tokio::test]
    async fn streamed_puffin_matches_existing_encoding() {
        use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
        for codec in [CompressionCodec::None, CompressionCodec::zstd_default()] {
            let temp = tempfile::tempdir().unwrap();
            let output = test_output(&temp, "stream.puffin");
            let raw = b"test payload with repeated words repeated words";
            let encoded = match codec {
                CompressionCodec::None => raw.to_vec(),
                CompressionCodec::Zstd(level) => {
                    let compressed = compress_v1(raw.as_slice(), raw.len() as u64, level).unwrap();
                    let mut bytes = Vec::new();
                    compressed
                        .reopen()
                        .unwrap()
                        .read_to_end(&mut bytes)
                        .unwrap();
                    bytes
                }
                _ => unreachable!(),
            };
            let chunks = encoded
                .chunks(3)
                .map(|part| Ok(Bytes::copy_from_slice(part)))
                .collect::<Vec<_>>();
            let mut writer = PuffinWriter::new(&output, Default::default(), false)
                .await
                .unwrap();
            writer
                .add_compressed_stream(
                    blob(Vec::new()),
                    codec,
                    encoded.len() as u64,
                    futures::stream::iter(chunks),
                )
                .await
                .unwrap();
            writer.close().await.unwrap();
            let reader = PuffinReader::new(output.to_input_file());
            let metadata = reader.file_metadata().await.unwrap();
            assert_eq!(metadata.blobs().len(), 1);
            assert_eq!(reader.blob(&metadata.blobs()[0]).await.unwrap().data(), raw);
            let ordinary = test_output(&temp, "ordinary.puffin");
            let mut writer = PuffinWriter::new(&ordinary, Default::default(), false)
                .await
                .unwrap();
            writer.add(blob(raw.to_vec()), codec).await.unwrap();
            writer.close().await.unwrap();
            assert_eq!(
                output.to_input_file().read().await.unwrap(),
                ordinary.to_input_file().read().await.unwrap()
            );
        }
    }

    #[tokio::test]
    async fn streamed_puffin_length_and_input_failures_poison_writer() {
        use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
        for (declared, fail) in [(2, false), (4, false), (3, true)] {
            let temp = tempfile::tempdir().unwrap();
            let output = test_output(&temp, "failed.puffin");
            let mut writer = PuffinWriter::new(&output, Default::default(), false)
                .await
                .unwrap();
            let chunk = if fail {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "injected stream error",
                ))
            } else {
                Ok(Bytes::from_static(b"abc"))
            };
            assert!(writer
                .add_compressed_stream(
                    blob(Vec::new()),
                    CompressionCodec::None,
                    declared,
                    futures::stream::iter([chunk])
                )
                .await
                .is_err());
            assert!(writer
                .add(blob(b"abc".to_vec()), CompressionCodec::None)
                .await
                .is_err());
            assert!(writer.close().await.is_err());
            assert!(PuffinReader::new(output.to_input_file())
                .file_metadata()
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn streamed_puffin_cancelled_future_cannot_finalize() {
        use futures::{FutureExt, StreamExt};
        use iceberg::puffin::{CompressionCodec, PuffinWriter};
        let temp = tempfile::tempdir().unwrap();
        let output = test_output(&temp, "cancelled.puffin");
        let mut writer = PuffinWriter::new(&output, Default::default(), false)
            .await
            .unwrap();
        let stream = futures::stream::iter([Ok(Bytes::from_static(b"abc"))])
            .chain(futures::stream::pending());
        // Poll to an asynchronous suspension, then drop the future. Even
        // cancellation during header I/O must not finalize a partial blob.
        assert!(writer
            .add_compressed_stream(blob(Vec::new()), CompressionCodec::None, 3, stream)
            .now_or_never()
            .is_none());
        assert!(writer.close().await.is_err());
    }

    #[tokio::test]
    async fn streamed_puffin_rejects_duplicate_payload_and_bad_codec() {
        use iceberg::puffin::{CompressionCodec, PuffinWriter};
        let temp = tempfile::tempdir().unwrap();
        let output = test_output(&temp, "validation.puffin");
        let mut writer = PuffinWriter::new(&output, Default::default(), false)
            .await
            .unwrap();
        assert!(writer
            .add_compressed_stream(
                blob(b"abc".to_vec()),
                CompressionCodec::None,
                0,
                futures::stream::empty()
            )
            .await
            .is_err());
        assert!(writer
            .add_compressed_stream(
                blob(Vec::new()),
                CompressionCodec::gzip_default(),
                0,
                futures::stream::empty()
            )
            .await
            .is_err());
        // Rejection before any streamed I/O does not poison the writer.
        writer
            .add(blob(b"abc".to_vec()), CompressionCodec::None)
            .await
            .unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn streamed_puffin_file_limit_includes_footer() {
        use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
        let temp = tempfile::tempdir().unwrap();
        let output = test_output(&temp, "limited.puffin");
        let mut writer = PuffinWriter::new(&output, Default::default(), false)
            .await
            .unwrap()
            .with_max_size(7);
        writer
            .add_compressed_stream(
                blob(Vec::new()),
                CompressionCodec::None,
                3,
                futures::stream::iter([Ok(Bytes::from_static(b"abc"))]),
            )
            .await
            .unwrap();
        assert!(writer.close().await.is_err());
        assert!(output.to_input_file().metadata().await.unwrap().size <= 7);
        assert!(PuffinReader::new(output.to_input_file())
            .file_metadata()
            .await
            .is_err());
    }
}
