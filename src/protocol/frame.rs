//! Request and response frame encoding/decoding.
//!
//! ```text
//! Request:  [total_length:4][request_id:8][op_code:2][flags:2][payload]
//! Response: [total_length:4][request_id:8][status:1][payload]
//! ```

use crate::protocol::opcodes::MAX_FRAME_SIZE;
use bytes::Bytes;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameError {
    #[error("frame too short: need {need}, have {have}")]
    TooShort { need: usize, have: usize },
    #[error("frame too large: {size} exceeds max {max}")]
    TooLarge { size: u32, max: u32 },
    #[error("truncated frame: declared {declared}, actual {actual}")]
    Truncated { declared: usize, actual: usize },
    /// The declared `total_length` is below the minimum required for the
    /// smallest valid frame (length prefix + at least one opcode byte).
    /// Enforced BEFORE any allocation so malformed frames cannot drive
    /// resource consumption.
    #[error("frame below minimum size: total_length {total_length} < minimum {minimum}")]
    BelowMinimum { total_length: u32, minimum: u32 },
}

/// Minimum `total_length` for any frame: at least 1 opcode byte after the
/// 4-byte length prefix. The per-direction minimums ([`MIN_REQUEST_BODY`]
/// and [`MIN_RESPONSE_BODY`]) are stricter, but this absolute floor is
/// checked first so that grossly malformed inputs are rejected before
/// any payload allocation. Value: 1 (a single opcode byte).
pub const MIN_FRAME_BODY: u32 = 1;

/// Minimum `total_length` for a request frame: `request_id(8) + op_code(2) + flags(2)`.
pub const MIN_REQUEST_BODY: u32 = 8 + 2 + 2;

/// Minimum `total_length` for a response frame: `request_id(8) + status(1)`.
pub const MIN_RESPONSE_BODY: u32 = 8 + 1;

pub type Result<T> = std::result::Result<T, FrameError>;

/// Request header size: total_length(4) + request_id(8) + op_code(2) + flags(2) = 16
/// But total_length covers everything after itself, so the header fields
/// inside the length are: request_id(8) + op_code(2) + flags(2) = 12
pub const REQUEST_HEADER_SIZE: usize = 4 + 8 + 2 + 2; // 16 bytes on wire

/// Response header size: total_length(4) + request_id(8) + status(1) = 13
pub const RESPONSE_HEADER_SIZE: usize = 4 + 8 + 1; // 13 bytes on wire

/// A decoded request frame.
///
/// `payload` is a [`Bytes`] handle — it can be a zero-copy slice of the
/// connection's read buffer (`decode_bytes`) or an owned `Vec` (`decode`).
/// `Bytes` implements `Deref<Target=[u8]>`, so handlers continue to use
/// it as a byte slice via `&req.payload[..]` or `&*req.payload` without
/// any code change. C-6 / F-G5-011 (P3.4) — eliminates the per-frame
/// `Vec<u8>::to_vec()` clone that used to live in `decode`.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestFrame {
    /// Client-assigned request ID for pipelining.
    pub request_id: u64,
    /// Operation code.
    pub op_code: u16,
    /// Flags (reserved).
    pub flags: u16,
    /// Operation-specific payload. Held as [`Bytes`] so zero-copy
    /// slicing from the connection read buffer is possible (see
    /// [`RequestFrame::decode_bytes`]).
    pub payload: Bytes,
}

/// A decoded response frame.
#[derive(Debug, Clone, PartialEq)]
pub struct ResponseFrame {
    /// Matches the request's request_id.
    pub request_id: u64,
    /// Response status.
    pub status: u8,
    /// Operation-specific payload.
    pub payload: Vec<u8>,
}

impl RequestFrame {
    /// Encode this frame to bytes.
    ///
    /// The `total_length` is computed from the payload size.
    pub fn encode(&self) -> Vec<u8> {
        let inner_len = 8 + 2 + 2 + self.payload.len(); // request_id + op_code + flags + payload
        let total_length = inner_len as u32;
        let mut buf = Vec::with_capacity(4 + inner_len);
        buf.extend_from_slice(&total_length.to_le_bytes());
        buf.extend_from_slice(&self.request_id.to_le_bytes());
        buf.extend_from_slice(&self.op_code.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Decode a request frame from a borrowed byte slice.
    ///
    /// Returns the frame and the total number of bytes consumed. The
    /// payload is copied into an owned `Bytes` because the input is
    /// borrowed. Hot-path callers should prefer [`Self::decode_bytes`],
    /// which takes a shared [`Bytes`] read buffer and produces a
    /// zero-copy payload slice (C-6 / F-G5-011 / P3.4).
    ///
    /// # Errors
    ///
    /// Rejects frames smaller than `4 (length prefix) + 1 (opcode)` bytes
    /// BEFORE any payload allocation via [`FrameError::BelowMinimum`].
    /// Returns [`FrameError::TooLarge`] for frames exceeding the wire
    /// maximum, [`FrameError::Truncated`] for declared lengths that
    /// exceed the buffer, and [`FrameError::TooShort`] when the
    /// remaining body cannot contain the fixed request header fields.
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        let (request_id, op_code, flags, payload_range, frame_size) = parse_request_header(data)?;
        let payload = Bytes::copy_from_slice(&data[payload_range]);
        Ok((
            Self {
                request_id,
                op_code,
                flags,
                payload,
            },
            frame_size,
        ))
    }

    /// Zero-copy decode of a request frame from a shared [`Bytes`] read
    /// buffer.
    ///
    /// The returned `RequestFrame::payload` is a `Bytes::slice(...)` of
    /// the input — it shares the underlying allocation via reference
    /// counting and does not copy. Designed for the server connection
    /// loop, which holds a per-connection [`bytes::BytesMut`] and freezes
    /// each completed frame into `Bytes` before decoding.
    ///
    /// C-6 / F-G5-011 (P3.4) — replaces the `payload[..].to_vec()` copy
    /// in [`Self::decode`] for the hot opcode path.
    ///
    /// # Errors
    ///
    /// Same error variants as [`Self::decode`].
    pub fn decode_bytes(data: Bytes) -> Result<(Self, usize)> {
        let (request_id, op_code, flags, payload_range, frame_size) = parse_request_header(&data)?;
        let payload = data.slice(payload_range);
        Ok((
            Self {
                request_id,
                op_code,
                flags,
                payload,
            },
            frame_size,
        ))
    }
}

/// Validate a request frame header and return the parsed fixed fields,
/// the payload byte range within `data`, and the total frame size on
/// the wire (length prefix + body). Shared between `decode` (borrowed)
/// and `decode_bytes` (shared `Bytes`) so the validation logic stays
/// single-source.
fn parse_request_header(data: &[u8]) -> Result<(u64, u16, u16, std::ops::Range<usize>, usize)> {
    if data.len() < 4 {
        return Err(FrameError::TooShort {
            need: 4,
            have: data.len(),
        });
    }
    let total_length =
        u32::from_le_bytes(data[0..4].try_into().map_err(|_| FrameError::TooShort {
            need: 4,
            have: data.len(),
        })?);
    if total_length < MIN_FRAME_BODY {
        return Err(FrameError::BelowMinimum {
            total_length,
            minimum: MIN_FRAME_BODY,
        });
    }
    if total_length > MAX_FRAME_SIZE {
        return Err(FrameError::TooLarge {
            size: total_length,
            max: MAX_FRAME_SIZE,
        });
    }
    let frame_size = 4 + total_length as usize;
    if data.len() < frame_size {
        return Err(FrameError::Truncated {
            declared: frame_size,
            actual: data.len(),
        });
    }
    if total_length < MIN_REQUEST_BODY {
        return Err(FrameError::TooShort {
            need: MIN_REQUEST_BODY as usize,
            have: total_length as usize,
        });
    }
    let request_id =
        u64::from_le_bytes(data[4..12].try_into().map_err(|_| FrameError::Truncated {
            declared: frame_size,
            actual: data.len(),
        })?);
    let op_code =
        u16::from_le_bytes(data[12..14].try_into().map_err(|_| FrameError::Truncated {
            declared: frame_size,
            actual: data.len(),
        })?);
    let flags = u16::from_le_bytes(data[14..16].try_into().map_err(|_| FrameError::Truncated {
        declared: frame_size,
        actual: data.len(),
    })?);
    Ok((request_id, op_code, flags, 16..frame_size, frame_size))
}

impl ResponseFrame {
    /// Encode this frame to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let inner_len = 8 + 1 + self.payload.len(); // request_id + status + payload
        let total_length = inner_len as u32;
        let mut buf = Vec::with_capacity(4 + inner_len);
        buf.extend_from_slice(&total_length.to_le_bytes());
        buf.extend_from_slice(&self.request_id.to_le_bytes());
        buf.push(self.status);
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Decode a response frame from bytes.
    ///
    /// # Errors
    ///
    /// Rejects frames smaller than `4 (length prefix) + 1 (opcode)` bytes
    /// BEFORE any payload allocation via [`FrameError::BelowMinimum`].
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 4 {
            return Err(FrameError::TooShort {
                need: 4,
                have: data.len(),
            });
        }
        let total_length =
            u32::from_le_bytes(data[0..4].try_into().map_err(|_| FrameError::TooShort {
                need: 4,
                have: data.len(),
            })?);
        if total_length < MIN_FRAME_BODY {
            return Err(FrameError::BelowMinimum {
                total_length,
                minimum: MIN_FRAME_BODY,
            });
        }
        if total_length > MAX_FRAME_SIZE {
            return Err(FrameError::TooLarge {
                size: total_length,
                max: MAX_FRAME_SIZE,
            });
        }
        let frame_size = 4 + total_length as usize;
        if data.len() < frame_size {
            return Err(FrameError::Truncated {
                declared: frame_size,
                actual: data.len(),
            });
        }
        if total_length < MIN_RESPONSE_BODY {
            return Err(FrameError::TooShort {
                need: MIN_RESPONSE_BODY as usize,
                have: total_length as usize,
            });
        }
        let request_id =
            u64::from_le_bytes(data[4..12].try_into().map_err(|_| FrameError::Truncated {
                declared: frame_size,
                actual: data.len(),
            })?);
        let status = data[12];
        let payload = data[13..frame_size].to_vec();

        Ok((
            Self {
                request_id,
                status,
                payload,
            },
            frame_size,
        ))
    }
}

/// Decode multiple frames from a byte stream, distinguishing partial
/// reads (retry) from corrupt input (disconnect).
///
/// F-G5-020 / H-01: the legacy `decode_frames` helper (since deleted)
/// swallowed any inner [`FrameError`], so a malformed trailing frame was
/// indistinguishable from "the buffer ends mid-frame, give me more
/// bytes". This helper surfaces the distinction:
///
/// - On [`FrameError::Truncated`] (declared length exceeds the buffer),
///   returns `Ok((frames_so_far, pos))` — caller refills the buffer.
/// - On any other [`FrameError`] (`TooShort`, `TooLarge`, `BelowMinimum`),
///   returns `Err(error)` — the stream is corrupt, the caller must
///   disconnect rather than retry.
pub fn try_decode_frames(data: &[u8]) -> Result<(Vec<RequestFrame>, usize)> {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        match RequestFrame::decode(&data[pos..]) {
            Ok((frame, consumed)) => {
                frames.push(frame);
                pos += consumed;
            }
            Err(FrameError::Truncated { .. }) => break,
            Err(e) => return Err(e),
        }
    }
    Ok((frames, pos))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::opcodes::*;

    #[test]
    fn request_frame_round_trip() {
        let frame = RequestFrame {
            request_id: 42,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: Bytes::from_static(&[1, 2, 3, 4, 5]),
        };
        let encoded = frame.encode();
        let (decoded, consumed) = RequestFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(consumed, encoded.len());
    }

    /// P3.4 / C-6: `decode_bytes` slices the underlying allocation
    /// without copying. After decoding, the payload `Bytes` and the
    /// source `Bytes` share the same pointer for the payload range.
    #[test]
    fn request_frame_decode_bytes_is_zero_copy() {
        let frame = RequestFrame {
            request_id: 42,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: Bytes::from_static(&[1, 2, 3, 4, 5]),
        };
        let encoded = Bytes::from(frame.encode());
        let payload_start_in_buf = encoded.as_ptr() as usize + 16;
        let (decoded, _) = RequestFrame::decode_bytes(encoded.clone()).unwrap();
        // Pointer identity proves the payload shares the source allocation.
        assert_eq!(decoded.payload.as_ptr() as usize, payload_start_in_buf);
        assert_eq!(&*decoded.payload, &[1, 2, 3, 4, 5][..]);
    }

    #[test]
    fn response_frame_round_trip() {
        let frame = ResponseFrame {
            request_id: 99,
            status: STATUS_OK,
            payload: vec![0xAA, 0xBB],
        };
        let encoded = frame.encode();
        let (decoded, consumed) = ResponseFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn deterministic_frame_roundtrip_corpus() {
        for payload_len in [0usize, 1, 7, 12, 255, 4096] {
            let payload: Vec<u8> = (0..payload_len).map(|i| (i % 251) as u8).collect();
            let request = RequestFrame {
                request_id: 0xA5A5_0000 + payload_len as u64,
                op_code: OP_GET_BATCH,
                flags: payload_len as u16,
                payload: Bytes::from(payload.clone()),
            };
            let encoded = request.encode();
            let (decoded, consumed) = RequestFrame::decode(&encoded).unwrap();
            assert_eq!(decoded, request);
            assert_eq!(consumed, encoded.len());

            let response = ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload,
            };
            let encoded = response.encode();
            let (decoded, consumed) = ResponseFrame::decode(&encoded).unwrap();
            assert_eq!(decoded, response);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn max_payload_frame() {
        let frame = RequestFrame {
            request_id: 1,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: Bytes::from(vec![0u8; 1024 * 1024]), // 1 MB payload
        };
        let encoded = frame.encode();
        let (decoded, _) = RequestFrame::decode(&encoded).unwrap();
        assert_eq!(decoded.payload.len(), 1024 * 1024);
    }

    #[test]
    fn truncated_frame_error() {
        let frame = RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: Bytes::from(vec![0u8; 100]),
        };
        let encoded = frame.encode();
        let result = RequestFrame::decode(&encoded[..encoded.len() / 2]);
        // The length prefix declares the full frame but the buffer is half
        // that → FrameError::Truncated (N-LOW: assert the variant).
        assert!(
            matches!(result, Err(FrameError::Truncated { .. })),
            "expected Truncated for half-buffer decode, got {result:?}",
        );
    }

    #[test]
    fn too_large_frame_rejected() {
        let mut data = vec![0u8; 8];
        // Set total_length to MAX + 1
        let too_big = MAX_FRAME_SIZE + 1;
        data[0..4].copy_from_slice(&too_big.to_le_bytes());
        let result = RequestFrame::decode(&data);
        assert!(matches!(result, Err(FrameError::TooLarge { .. })));
    }

    #[test]
    fn malformed_frame_length_corpus_rejects_without_panic() {
        let corpus: &[&[u8]] = &[
            &[],
            &[0],
            &[0, 0],
            &[0, 0, 0],
            &0u32.to_le_bytes(),
            &5u32.to_le_bytes(),
            &(MIN_REQUEST_BODY - 1).to_le_bytes(),
            &(MAX_FRAME_SIZE + 1).to_le_bytes(),
        ];

        // Each corpus entry trips a different length guard (too-short prefix,
        // below-minimum body, or too-large declared length); pin that every
        // one is rejected with a typed FrameError of the malformed family —
        // not a bare is_err() (N-LOW).
        fn is_malformed_frame_error(e: &FrameError) -> bool {
            matches!(
                e,
                FrameError::TooShort { .. }
                    | FrameError::BelowMinimum { .. }
                    | FrameError::TooLarge { .. }
                    | FrameError::Truncated { .. }
            )
        }
        for data in corpus {
            match RequestFrame::decode(data) {
                Err(e) => assert!(
                    is_malformed_frame_error(&e),
                    "request decode of {data:?} returned unexpected error {e:?}",
                ),
                Ok(_) => panic!("malformed request frame {data:?} must be rejected"),
            }
            match ResponseFrame::decode(data) {
                Err(e) => assert!(
                    is_malformed_frame_error(&e),
                    "response decode of {data:?} returned unexpected error {e:?}",
                ),
                Ok(_) => panic!("malformed response frame {data:?} must be rejected"),
            }
        }

        let mut truncated_request = Vec::new();
        truncated_request.extend_from_slice(&MIN_REQUEST_BODY.to_le_bytes());
        truncated_request.extend_from_slice(&[0u8; (MIN_REQUEST_BODY - 1) as usize]);
        assert!(matches!(
            RequestFrame::decode(&truncated_request),
            Err(FrameError::Truncated { .. })
        ));
    }

    #[test]
    fn multiple_frames_in_stream() {
        let f1 = RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: Bytes::new(),
        };
        let f2 = RequestFrame {
            request_id: 2,
            op_code: OP_HEALTH,
            flags: 0,
            payload: Bytes::from_static(&[42]),
        };
        let mut stream = f1.encode();
        stream.extend_from_slice(&f2.encode());

        let (frames, consumed) =
            try_decode_frames(&stream).expect("two well-formed frames should decode");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].request_id, 1);
        assert_eq!(frames[1].request_id, 2);
        assert_eq!(consumed, stream.len());
    }

    /// F-G5-020: `try_decode_frames` returns `Ok` on a truncated tail
    /// (caller should refill) but propagates other `FrameError`s so a
    /// corrupt trailing frame is distinguishable from a partial one.
    #[test]
    fn try_decode_frames_surfaces_corrupt_trailing_frame() {
        let f1 = RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: Bytes::new(),
        };
        let mut stream = f1.encode();
        // Push a length prefix declaring a frame that exceeds MAX_FRAME_SIZE
        // — corrupt rather than partial.
        let too_large_len = (MAX_FRAME_SIZE + 1).to_le_bytes();
        stream.extend_from_slice(&too_large_len);
        // Plus a couple of body bytes so the buffer is non-empty after the
        // length prefix (otherwise the decode reports a different error).
        stream.extend_from_slice(&[0u8; 32]);
        match try_decode_frames(&stream) {
            Err(FrameError::TooLarge { .. }) => {}
            other => panic!("expected TooLarge error, got {other:?}"),
        }
    }

    /// F-G5-020: a real partial tail (declared length exceeds the buffer)
    /// is reported via `Ok((frames_so_far, pos))` so the caller knows to
    /// refill the buffer rather than disconnect.
    #[test]
    fn try_decode_frames_returns_ok_on_partial_tail() {
        let f1 = RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: Bytes::new(),
        };
        let f2_full = RequestFrame {
            request_id: 2,
            op_code: OP_HEALTH,
            flags: 0,
            payload: Bytes::from_static(&[42]),
        }
        .encode();
        let mut stream = f1.encode();
        // Append only a prefix of f2 — exercises FrameError::Truncated.
        stream.extend_from_slice(&f2_full[..f2_full.len() - 2]);
        let (frames, consumed) =
            try_decode_frames(&stream).expect("truncated tail should not be a hard error");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].request_id, 1);
        assert!(consumed < stream.len());
    }

    #[test]
    fn partial_error_response() {
        let frame = ResponseFrame {
            request_id: 7,
            status: STATUS_PARTIAL_ERROR,
            payload: {
                let mut p = Vec::new();
                // error_count: 2
                p.extend_from_slice(&2u32.to_le_bytes());
                // Error 1: item_index=3, error_code=TX_NOT_FOUND, data_len=0
                p.extend_from_slice(&3u32.to_le_bytes());
                p.extend_from_slice(&ERR_TX_NOT_FOUND.to_le_bytes());
                p.extend_from_slice(&0u16.to_le_bytes());
                // Error 2: item_index=7, error_code=ALREADY_SPENT, data_len=36
                p.extend_from_slice(&7u32.to_le_bytes());
                p.extend_from_slice(&ERR_ALREADY_SPENT.to_le_bytes());
                p.extend_from_slice(&36u16.to_le_bytes());
                p.extend_from_slice(&[0xAB; 36]);
                p
            },
        };
        let encoded = frame.encode();
        let (decoded, _) = ResponseFrame::decode(&encoded).unwrap();
        assert_eq!(decoded.status, STATUS_PARTIAL_ERROR);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn request_header_size() {
        let frame = RequestFrame {
            request_id: 0,
            op_code: 0,
            flags: 0,
            payload: Bytes::new(),
        };
        let encoded = frame.encode();
        assert_eq!(encoded.len(), REQUEST_HEADER_SIZE); // 16 bytes total for empty payload
    }

    #[test]
    fn response_header_size() {
        let frame = ResponseFrame {
            request_id: 0,
            status: 0,
            payload: vec![],
        };
        let encoded = frame.encode();
        assert_eq!(encoded.len(), RESPONSE_HEADER_SIZE); // 13 bytes total for empty payload
    }

    #[test]
    fn decode_frame_shorter_than_minimum_returns_error() {
        // Request: declared total_length = 0 → below the 1-byte minimum.
        let data = 0u32.to_le_bytes();
        let err = RequestFrame::decode(&data).unwrap_err();
        match err {
            FrameError::BelowMinimum {
                total_length,
                minimum,
            } => {
                assert_eq!(total_length, 0);
                assert_eq!(minimum, MIN_FRAME_BODY);
            }
            other => panic!("expected BelowMinimum, got {other:?}"),
        }

        // Response: same floor applies.
        let err = ResponseFrame::decode(&data).unwrap_err();
        match err {
            FrameError::BelowMinimum {
                total_length,
                minimum,
            } => {
                assert_eq!(total_length, 0);
                assert_eq!(minimum, MIN_FRAME_BODY);
            }
            other => panic!("expected BelowMinimum, got {other:?}"),
        }
    }

    #[test]
    fn decode_request_below_header_size_returns_too_short() {
        // total_length = 5 passes the absolute minimum but is still below
        // the request header's 12-byte requirement. Must return TooShort
        // after the minimum guard has already run.
        let mut data = Vec::new();
        data.extend_from_slice(&5u32.to_le_bytes()); // total_length
        data.extend_from_slice(&[0u8; 5]); // body
        let err = RequestFrame::decode(&data).unwrap_err();
        match err {
            FrameError::TooShort { need, have } => {
                assert_eq!(need, MIN_REQUEST_BODY as usize);
                assert_eq!(have, 5);
            }
            other => panic!("expected TooShort, got {other:?}"),
        }
    }

    #[test]
    fn total_length_computed_correctly() {
        let frame = RequestFrame {
            request_id: 42,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: Bytes::from(vec![0u8; 100]),
        };
        let encoded = frame.encode();
        let total_length = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
        // total_length = request_id(8) + op_code(2) + flags(2) + payload(100) = 112
        assert_eq!(total_length, 112);
        assert_eq!(encoded.len(), 4 + 112);
    }
}
