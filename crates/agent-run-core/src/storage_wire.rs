//! Binary framing of the private writer socket (`writer.sock`), shared by the
//! broker's server side and every client.
//!
//! Every frame is a fixed 24-byte big-endian header followed by `payload_len`
//! body bytes. The header carries the operation's stable wire id, so a receiver
//! can bound the body (`wire_max` of that operation) and reserve its memory
//! BEFORE reading a single payload byte; an oversize or unknown frame is
//! refused without buffering it. The body is the canonical JSON owned by the
//! storage protocol; this module never interprets it, never trusts a role or
//! authority field and never executes a partial operation: a request is handed
//! on only after its whole body was received.
//!
//! ```text
//! offset size field
//!      0    4 magic "ARWQ"
//!      4    2 protocol version (1)
//!      6    1 message type (see [`Message`])
//!      7    1 flags (reserved, must be 0; never grants class or authority)
//!      8    2 operation wire id (0 for non-operation messages)
//!     10    2 reserved, must be 0
//!     12    4 payload length in bytes
//!     16    8 sequence number chosen by the sender, echoed by the reply
//! ```
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Fixed header size in bytes.
pub const HEADER_LEN: usize = 24;
/// Frame magic.
pub const MAGIC: [u8; 4] = *b"ARWQ";
/// Current framing protocol version.
pub const PROTOCOL: u16 = 1;
/// Hard ceiling for any frame body, independent of the per-operation table.
pub const BODY_HARD_MAX: usize = 16 * 1024 * 1024 + 64 * 1024;
/// Largest hello / hello-ok body. A supervisor hello may carry its finish
/// launch-secret context, which is bounded by [`FINISH_GUARD_MAX`].
pub const HELLO_MAX: usize = 16 * 1024;
/// Largest encoded finish launch-secret context a supervisor hello may carry,
/// leaving room for the hello's other claims within [`HELLO_MAX`].
pub const FINISH_GUARD_MAX: usize = 12 * 1024;
/// Largest encoded reply or status reply (`72 KiB`, section 5a of the contract).
pub const REPLY_MAX: usize = 72 * 1024;
/// Largest status request body (operation id plus digest).
pub const STATUS_MAX: usize = 1024;

/// Message type byte of a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Message {
    /// Client introduction: protocol, claimed role, agent/attempt claim, capability.
    Hello = 1,
    /// Server acknowledgement carrying the writer epoch and clock sample.
    HelloOk = 2,
    /// One operation envelope.
    Request = 3,
    /// One encoded `SubmitResult`.
    Reply = 4,
    /// Operation status lookup.
    Status = 5,
    /// One encoded `OpStatus`.
    StatusReply = 6,
}

impl Message {
    /// Decode a message byte; unknown values are refused.
    fn from_byte(byte: u8) -> Option<Self> {
        Some(match byte {
            1 => Self::Hello,
            2 => Self::HelloOk,
            3 => Self::Request,
            4 => Self::Reply,
            5 => Self::Status,
            6 => Self::StatusReply,
            _ => return None,
        })
    }
}

/// Decoded frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Message type.
    pub message: Message,
    /// Operation wire id for requests; zero otherwise.
    pub op_kind: u16,
    /// Body length in bytes.
    pub payload_len: u32,
    /// Sender-chosen sequence, echoed by the matching reply.
    pub seq: u64,
}

/// Why a frame was refused or could not be read.
#[derive(Debug)]
pub enum FrameError {
    /// The magic bytes differ; the stream is not this protocol.
    BadMagic,
    /// The peer speaks another protocol version.
    UnsupportedVersion(u16),
    /// Unknown message type, non-zero flags or non-zero reserved field.
    Malformed,
    /// The receiver accepts no frame of this message/operation kind at all, not
    /// even an empty one.
    Forbidden {
        /// Message type of the refused frame.
        message: Message,
        /// Operation wire id of the refused frame.
        op_kind: u16,
    },
    /// The declared body exceeds the bound for this message/operation.
    TooLarge {
        /// Declared body length.
        len: usize,
        /// Applicable maximum.
        max: usize,
    },
    /// The stream ended inside a header or body.
    Truncated,
    /// The underlying transport failed.
    Io(io::Error),
}

impl std::fmt::Display for FrameError {
    /// Closed, secret-free description of the refusal.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMagic => f.write_str("writer frame magic mismatch"),
            Self::UnsupportedVersion(version) => {
                write!(f, "writer frame version {version} is unsupported")
            }
            Self::Malformed => f.write_str("writer frame header is malformed"),
            Self::Forbidden { message, op_kind } => {
                write!(f, "writer frame {message:?}/{op_kind} is not accepted here")
            }
            Self::TooLarge { len, max } => {
                write!(f, "writer frame body {len} exceeds its maximum {max}")
            }
            Self::Truncated => f.write_str("writer frame is truncated"),
            Self::Io(error) => write!(f, "writer transport failed: {}", error.kind()),
        }
    }
}

impl std::error::Error for FrameError {}

/// Encode a header into its 24 wire bytes.
pub fn encode_header(header: &Header) -> [u8; HEADER_LEN] {
    let mut bytes = [0u8; HEADER_LEN];
    bytes[0..4].copy_from_slice(&MAGIC);
    bytes[4..6].copy_from_slice(&PROTOCOL.to_be_bytes());
    bytes[6] = header.message as u8;
    bytes[8..10].copy_from_slice(&header.op_kind.to_be_bytes());
    bytes[12..16].copy_from_slice(&header.payload_len.to_be_bytes());
    bytes[16..24].copy_from_slice(&header.seq.to_be_bytes());
    bytes
}

/// Decode and validate 24 header bytes.
///
/// # Errors
/// Magic, version, message type, flags, reserved-field and message/operation-id
/// matrix violations (a request must name a nonzero operation, every other
/// message must carry zero); the body
/// length is NOT bounded here (see [`read_header`]).
pub fn decode_header(bytes: &[u8; HEADER_LEN]) -> Result<Header, FrameError> {
    if bytes[0..4] != MAGIC {
        return Err(FrameError::BadMagic);
    }
    let version = u16::from_be_bytes([bytes[4], bytes[5]]);
    if version != PROTOCOL {
        return Err(FrameError::UnsupportedVersion(version));
    }
    let message = Message::from_byte(bytes[6]).ok_or(FrameError::Malformed)?;
    if bytes[7] != 0 || bytes[10] != 0 || bytes[11] != 0 {
        return Err(FrameError::Malformed);
    }
    let op_kind = u16::from_be_bytes([bytes[8], bytes[9]]);
    // The operation wire id belongs to requests only: a request names exactly one
    // operation (ids start at 1) and every other message type carries zero.
    if (message == Message::Request) != (op_kind != 0) {
        return Err(FrameError::Malformed);
    }
    Ok(Header {
        message,
        op_kind,
        payload_len: u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        seq: u64::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]),
    })
}

/// Read one header, refusing an oversize body BEFORE any body byte is read.
///
/// `max_body` maps the decoded message and operation id to the largest body the
/// receiver will accept; `None` refuses that message/operation outright with
/// [`FrameError::Forbidden`], even when the declared body is empty. The hard
/// ceiling [`BODY_HARD_MAX`] always applies. Returns `Ok(None)` on a clean
/// end of stream between frames.
///
/// # Errors
/// [`FrameError::TooLarge`] for an oversize body, [`FrameError::Forbidden`] for
/// an unaccepted kind, other variants for malformed or truncated input. After any error the stream cannot
/// be resynchronized and must be closed.
pub async fn read_header<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_body: impl Fn(Message, u16) -> Option<usize>,
) -> Result<Option<Header>, FrameError> {
    let mut bytes = [0u8; HEADER_LEN];
    let mut filled = 0;
    while filled < HEADER_LEN {
        match reader.read(&mut bytes[filled..]).await {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => return Err(FrameError::Truncated),
            Ok(read) => filled += read,
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    let header = decode_header(&bytes)?;
    let len = header.payload_len as usize;
    let Some(max) = max_body(header.message, header.op_kind) else {
        return Err(FrameError::Forbidden {
            message: header.message,
            op_kind: header.op_kind,
        });
    };
    let max = max.min(BODY_HARD_MAX);
    if len > max {
        return Err(FrameError::TooLarge { len, max });
    }
    Ok(Some(header))
}

/// Read exactly the body announced by an already validated `header`.
///
/// # Errors
/// [`FrameError::Truncated`] when the stream ends early.
pub async fn read_body<R: AsyncRead + Unpin>(
    reader: &mut R,
    header: &Header,
) -> Result<Vec<u8>, FrameError> {
    let mut body = vec![0u8; header.payload_len as usize];
    match reader.read_exact(&mut body).await {
        Ok(_) => Ok(body),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Err(FrameError::Truncated),
        Err(error) => Err(FrameError::Io(error)),
    }
}

/// Write one frame (header, then body) and flush it.
///
/// # Errors
/// The underlying write error, or `InvalidInput` when `body` exceeds `u32::MAX`
/// or [`BODY_HARD_MAX`].
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: Message,
    op_kind: u16,
    seq: u64,
    body: &[u8],
) -> io::Result<()> {
    if body.len() > BODY_HARD_MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "writer frame body exceeds the hard maximum",
        ));
    }
    let header = Header {
        message,
        op_kind,
        payload_len: body.len() as u32,
        seq,
    };
    writer.write_all(&encode_header(&header)).await?;
    writer.write_all(body).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    //! Bounds are enforced before payload reads; malformed headers are refused.
    use super::*;

    /// Request limits used by the tests: kind 7 allows 100 bytes, others none.
    fn limits(message: Message, kind: u16) -> Option<usize> {
        match (message, kind) {
            (Message::Request, 7) => Some(100),
            (Message::Hello, 0) => Some(HELLO_MAX),
            _ => None,
        }
    }

    /// A written frame reads back identically.
    #[tokio::test]
    async fn round_trips_a_request() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        write_frame(&mut a, Message::Request, 7, 42, b"hello")
            .await
            .unwrap();
        let header = read_header(&mut b, limits).await.unwrap().unwrap();
        assert_eq!(header.seq, 42);
        assert_eq!(header.op_kind, 7);
        assert_eq!(read_body(&mut b, &header).await.unwrap(), b"hello");
    }

    /// An oversize declaration is refused after reading only the 24 header bytes.
    #[tokio::test]
    async fn oversize_is_refused_before_the_body() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let header = Header {
            message: Message::Request,
            op_kind: 7,
            payload_len: 101,
            seq: 1,
        };
        a.write_all(&encode_header(&header)).await.unwrap();
        // No body is ever sent: a reader that waited for it would hang.
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_header(&mut b, limits),
        )
        .await
        .expect("must decide from the header alone")
        .unwrap_err();
        assert!(matches!(error, FrameError::TooLarge { len: 101, max: 100 }));
    }

    /// Unknown operations and message types are forbidden whatever their body
    /// length, including an empty body, before any body byte is read.
    #[tokio::test]
    async fn unknown_kinds_are_forbidden_even_when_empty() {
        for (message, kind, body) in [
            (Message::Request, 9u16, &b"x"[..]),
            (Message::Request, 9, &b""[..]),
            (Message::Status, 0, &b""[..]),
            (Message::Reply, 0, &b""[..]),
        ] {
            let (mut a, mut b) = tokio::io::duplex(4096);
            write_frame(&mut a, message, kind, 1, body).await.unwrap();
            assert!(
                matches!(
                    read_header(&mut b, limits).await,
                    Err(FrameError::Forbidden { .. })
                ),
                "{message:?}/{kind}/{}",
                body.len()
            );
        }
    }

    /// The operation id belongs to requests only: a request must name one and
    /// every other message type must carry zero, whatever the receiver allows.
    #[tokio::test]
    async fn operation_id_matrix_is_structural() {
        for (message, kind) in [
            (Message::Hello, 7u16),
            (Message::HelloOk, 7),
            (Message::Reply, 7),
            (Message::Status, 3),
            (Message::StatusReply, 1),
            (Message::Request, 0),
        ] {
            let bytes = encode_header(&Header {
                message,
                op_kind: kind,
                payload_len: 0,
                seq: 1,
            });
            assert!(
                matches!(decode_header(&bytes), Err(FrameError::Malformed)),
                "{message:?}/{kind}"
            );
            // Even a receiver that would allow everything cannot accept it.
            let (mut a, mut b) = tokio::io::duplex(4096);
            a.write_all(&bytes).await.unwrap();
            assert!(
                read_header(&mut b, |_, _| Some(usize::MAX)).await.is_err(),
                "{message:?}/{kind}"
            );
        }
        for (message, kind) in [
            (Message::Request, 1u16),
            (Message::Hello, 0),
            (Message::HelloOk, 0),
            (Message::Reply, 0),
            (Message::Status, 0),
            (Message::StatusReply, 0),
        ] {
            let bytes = encode_header(&Header {
                message,
                op_kind: kind,
                payload_len: 0,
                seq: 1,
            });
            assert!(decode_header(&bytes).is_ok(), "{message:?}/{kind}");
        }
    }

    /// Bad magic, version, flags and reserved bytes are all malformed.
    #[test]
    fn malformed_headers_are_refused() {
        let good = encode_header(&Header {
            message: Message::Hello,
            op_kind: 0,
            payload_len: 0,
            seq: 0,
        });
        assert!(decode_header(&good).is_ok());
        for (index, value) in [(0, b'X'), (5, 2), (6, 99), (7, 1), (10, 1), (11, 1)] {
            let mut bytes = good;
            bytes[index] = value;
            assert!(decode_header(&bytes).is_err(), "byte {index}");
        }
    }

    /// A stream ending inside the body is truncated, never a short success.
    #[tokio::test]
    async fn short_body_is_truncated() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let header = Header {
            message: Message::Hello,
            op_kind: 0,
            payload_len: 10,
            seq: 3,
        };
        a.write_all(&encode_header(&header)).await.unwrap();
        a.write_all(b"abc").await.unwrap();
        drop(a);
        let parsed = read_header(&mut b, limits).await.unwrap().unwrap();
        assert!(matches!(
            read_body(&mut b, &parsed).await,
            Err(FrameError::Truncated)
        ));
    }

    /// A clean end between frames is `None`.
    #[tokio::test]
    async fn clean_eof_between_frames() {
        let (a, mut b) = tokio::io::duplex(64);
        drop(a);
        assert!(read_header(&mut b, limits).await.unwrap().is_none());
    }
}
