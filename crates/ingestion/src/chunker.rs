use sha2::{Digest, Sha256};

use crate::{IngestionError, Result};

const CHUNKER_VERSION: &str = "northbridge-char-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextChunk {
    pub ordinal: u32,
    pub content: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub character_count: u32,
}

pub fn chunker_hash(chunk_size: u32, chunk_overlap: u32) -> Result<String> {
    validate_configuration(chunk_size, chunk_overlap)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(format!("{CHUNKER_VERSION}:{chunk_size}:{chunk_overlap}").as_bytes())
    ))
}

pub fn chunk_text(text: &str, chunk_size: u32, chunk_overlap: u32) -> Result<Vec<TextChunk>> {
    validate_configuration(chunk_size, chunk_overlap)?;
    let text = text.trim_matches('\u{feff}');
    if text.trim().is_empty() {
        return Err(IngestionError::Parse(
            "document did not contain extractable text".to_string(),
        ));
    }
    let mut boundaries = text
        .char_indices()
        .map(|(offset, _)| offset)
        .collect::<Vec<_>>();
    boundaries.push(text.len());
    let character_total = boundaries.len().saturating_sub(1);
    let size = chunk_size as usize;
    let overlap = chunk_overlap as usize;
    let mut chunks = Vec::new();
    let mut start_character = 0_usize;
    while start_character < character_total {
        let target_end = start_character.saturating_add(size).min(character_total);
        let end_character = if target_end < character_total {
            choose_break(text, &boundaries, start_character, target_end, size)
        } else {
            target_end
        };
        let raw_start = boundaries[start_character];
        let raw_end = boundaries[end_character];
        let raw = &text[raw_start..raw_end];
        let leading = raw.len().saturating_sub(raw.trim_start().len());
        let trailing = raw.len().saturating_sub(raw.trim_end().len());
        let start = raw_start.saturating_add(leading);
        let end = raw_end.saturating_sub(trailing);
        if start < end {
            let content = text[start..end].to_string();
            chunks.push(TextChunk {
                ordinal: u32::try_from(chunks.len()).map_err(|_| {
                    IngestionError::InvalidInput("document produced too many chunks".to_string())
                })?,
                character_count: u32::try_from(content.chars().count()).unwrap_or(u32::MAX),
                content,
                start_offset: u64::try_from(start).unwrap_or(u64::MAX),
                end_offset: u64::try_from(end).unwrap_or(u64::MAX),
            });
        }
        if end_character >= character_total {
            break;
        }
        let next = end_character.saturating_sub(overlap);
        start_character = next.max(start_character.saturating_add(1));
        if chunks.len() > 100_000 {
            return Err(IngestionError::InvalidInput(
                "document produced more than 100,000 chunks".to_string(),
            ));
        }
    }
    if chunks.is_empty() {
        return Err(IngestionError::Parse(
            "document did not contain extractable text".to_string(),
        ));
    }
    Ok(chunks)
}

fn choose_break(
    text: &str,
    boundaries: &[usize],
    start: usize,
    target: usize,
    size: usize,
) -> usize {
    let floor = target.saturating_sub((size / 5).max(32)).max(start + 1);
    for character in (floor..target).rev() {
        let slice = &text[boundaries[character]..boundaries[character + 1]];
        if slice.chars().all(char::is_whitespace) {
            return character + 1;
        }
    }
    target
}

fn validate_configuration(chunk_size: u32, chunk_overlap: u32) -> Result<()> {
    if !(256..=8192).contains(&chunk_size) || chunk_overlap >= chunk_size {
        return Err(IngestionError::InvalidInput(
            "chunk size must be 256-8,192 and overlap must be smaller".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::chunk_text;

    #[test]
    fn creates_overlapping_unicode_safe_chunks_with_source_offsets() {
        let text = format!("{} {}", "åäö ".repeat(180), "tail");
        let chunks = chunk_text(&text, 256, 32).unwrap();
        assert!(chunks.len() > 2);
        assert_eq!(chunks[0].ordinal, 0);
        for chunk in chunks {
            let start = usize::try_from(chunk.start_offset).unwrap();
            let end = usize::try_from(chunk.end_offset).unwrap();
            assert_eq!(&text[start..end], chunk.content);
            assert!(text.is_char_boundary(start));
            assert!(text.is_char_boundary(end));
        }
    }
}
