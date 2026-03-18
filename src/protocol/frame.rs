//! Request and response frame encoding/decoding.
//!
//! Request: [total_length:4][request_id:8][op_code:2][flags:2][payload]
//! Response: [total_length:4][request_id:8][status:1][payload]

use crate::protocol::opcodes::MAX_FRAME_SIZE;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameError {
    #[error("frame too short: need {need}, have {have}")]
    TooShort { need: usize, have: usize },
    #[error("frame too large: {size} exceeds max {max}")]
    TooLarge { size: u32, max: u32 },
    #[error("truncated frame: declared {declared}, actual {actual}")]
    Truncated { declared: usize, actual: usize },
}

pub type Result<T> = std::result::Result<T, FrameError>;

/// Request header size: total_length(4) + request_id(8) + op_code(2) + flags(2) = 16
/// But total_length covers everything after itself, so the header fields
/// inside the length are: request_id(8) + op_code(2) + flags(2) = 12
pub const REQUEST_HEADER_SIZE: usize = 4 + 8 + 2 + 2; // 16 bytes on wire

/// Response header size: total_length(4) + request_id(8) + status(1) = 13
pub const RESPONSE_HEADER_SIZE: usize = 4 + 8 + 1; // 13 bytes on wire

/// A decoded request frame.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestFrame {
    /// Client-assigned request ID for pipelining.
    pub request_id: u64,
    /// Operation code.
    pub op_code: u16,
    /// Flags (reserved).
    pub flags: u16,
    /// Operation-specific payload.
    pub payload: Vec<u8>,
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

    /// Decode a request frame from bytes.
    ///
    /// Returns the frame and the total number of bytes consumed.
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 4 {
            return Err(FrameError::TooShort { need: 4, have: data.len() });
        }
        let total_length = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if total_length > MAX_FRAME_SIZE {
            return Err(FrameError::TooLarge { size: total_length, max: MAX_FRAME_SIZE });
        }
        let frame_size = 4 + total_length as usize;
        if data.len() < frame_size {
            return Err(FrameError::Truncated {
                declared: frame_size,
                actual: data.len(),
            });
        }
        if total_length < 12 {
            return Err(FrameError::TooShort { need: 12, have: total_length as usize });
        }
        let request_id = u64::from_le_bytes(data[4..12].try_into().unwrap());
        let op_code = u16::from_le_bytes(data[12..14].try_into().unwrap());
        let flags = u16::from_le_bytes(data[14..16].try_into().unwrap());
        let payload = data[16..frame_size].to_vec();

        Ok((Self { request_id, op_code, flags, payload }, frame_size))
    }
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
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 4 {
            return Err(FrameError::TooShort { need: 4, have: data.len() });
        }
        let total_length = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if total_length > MAX_FRAME_SIZE {
            return Err(FrameError::TooLarge { size: total_length, max: MAX_FRAME_SIZE });
        }
        let frame_size = 4 + total_length as usize;
        if data.len() < frame_size {
            return Err(FrameError::Truncated {
                declared: frame_size,
                actual: data.len(),
            });
        }
        if total_length < 9 {
            return Err(FrameError::TooShort { need: 9, have: total_length as usize });
        }
        let request_id = u64::from_le_bytes(data[4..12].try_into().unwrap());
        let status = data[12];
        let payload = data[13..frame_size].to_vec();

        Ok((Self { request_id, status, payload }, frame_size))
    }
}

/// Decode multiple frames from a byte stream.
///
/// Returns all complete frames found and the number of bytes consumed.
pub fn decode_frames(data: &[u8]) -> (Vec<RequestFrame>, usize) {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        match RequestFrame::decode(&data[pos..]) {
            Ok((frame, consumed)) => {
                frames.push(frame);
                pos += consumed;
            }
            Err(_) => break,
        }
    }
    (frames, pos)
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
            payload: vec![1, 2, 3, 4, 5],
        };
        let encoded = frame.encode();
        let (decoded, consumed) = RequestFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(consumed, encoded.len());
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
    fn max_payload_frame() {
        let frame = RequestFrame {
            request_id: 1,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: vec![0u8; 1024 * 1024], // 1 MB payload
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
            payload: vec![0u8; 100],
        };
        let encoded = frame.encode();
        let result = RequestFrame::decode(&encoded[..encoded.len() / 2]);
        assert!(result.is_err());
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
    fn multiple_frames_in_stream() {
        let f1 = RequestFrame { request_id: 1, op_code: OP_PING, flags: 0, payload: vec![] };
        let f2 = RequestFrame { request_id: 2, op_code: OP_HEALTH, flags: 0, payload: vec![42] };
        let mut stream = f1.encode();
        stream.extend_from_slice(&f2.encode());

        let (frames, consumed) = decode_frames(&stream);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].request_id, 1);
        assert_eq!(frames[1].request_id, 2);
        assert_eq!(consumed, stream.len());
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
        let frame = RequestFrame { request_id: 0, op_code: 0, flags: 0, payload: vec![] };
        let encoded = frame.encode();
        assert_eq!(encoded.len(), REQUEST_HEADER_SIZE); // 16 bytes total for empty payload
    }

    #[test]
    fn response_header_size() {
        let frame = ResponseFrame { request_id: 0, status: 0, payload: vec![] };
        let encoded = frame.encode();
        assert_eq!(encoded.len(), RESPONSE_HEADER_SIZE); // 13 bytes total for empty payload
    }

    #[test]
    fn total_length_computed_correctly() {
        let frame = RequestFrame {
            request_id: 42,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: vec![0u8; 100],
        };
        let encoded = frame.encode();
        let total_length = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
        // total_length = request_id(8) + op_code(2) + flags(2) + payload(100) = 112
        assert_eq!(total_length, 112);
        assert_eq!(encoded.len(), 4 + 112);
    }
}
