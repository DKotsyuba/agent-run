use crate::{Error,Result};
use tokio::io::{AsyncBufRead,AsyncBufReadExt,AsyncWrite,AsyncWriteExt};
/// Bounded LF framing. A partial frame at EOF is malformed, never silently ignored.
pub async fn read<R:AsyncBufRead+Unpin>(reader:&mut R,max:usize)->Result<Option<Vec<u8>>>{
    let mut line=Vec::new();
    loop{
        let available=reader.fill_buf().await?;
        if available.is_empty(){return if line.is_empty(){Ok(None)}else{Err(Error::Runtime("truncated JSON frame".into()))};}
        let end=available.iter().position(|b|*b==b'\n');let n=end.map(|i|i+1).unwrap_or(available.len());
        if line.len()+n>max{return Err(Error::Runtime("JSON frame exceeds maximum size".into()));}
        line.extend_from_slice(&available[..n]);reader.consume(n);
        if end.is_some(){line.pop();if line.last()==Some(&b'\r'){line.pop();}return Ok(Some(line));}
    }
}
pub async fn write<W:AsyncWrite+Unpin>(writer:&mut W,v:&serde_json::Value,max:usize)->Result<()> {
    let mut data=serde_json::to_vec(v)?;data.push(b'\n');
    if data.len()>max{return Err(Error::Runtime("JSON response exceeds maximum size".into()));}
    writer.write_all(&data).await?;writer.flush().await?;Ok(())
}
