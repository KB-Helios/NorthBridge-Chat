use std::{io::Read, path::Path};

use quick_xml::{Reader, events::Event};

use crate::{IngestionError, Result};

const MAX_ARCHIVE_MEMBER_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedDocument {
    pub title: String,
    pub mime_type: String,
    pub parser: String,
    pub text: String,
}

pub fn is_supported_document(path: &Path) -> bool {
    extension(path).is_some_and(|extension| {
        matches!(
            extension.as_str(),
            "txt"
                | "md"
                | "markdown"
                | "rst"
                | "csv"
                | "tsv"
                | "json"
                | "jsonl"
                | "yaml"
                | "yml"
                | "toml"
                | "log"
                | "html"
                | "htm"
                | "pdf"
                | "docx"
                | "odt"
                | "rs"
                | "py"
                | "js"
                | "jsx"
                | "ts"
                | "tsx"
                | "java"
                | "kt"
                | "swift"
                | "go"
                | "c"
                | "h"
                | "cpp"
                | "hpp"
                | "cs"
                | "sh"
                | "ps1"
                | "sql"
                | "xml"
        )
    })
}

pub fn extract_document(path: &Path, bytes: &[u8]) -> Result<ExtractedDocument> {
    let extension = extension(path).ok_or_else(|| {
        IngestionError::InvalidInput("document must have a supported extension".to_string())
    })?;
    if !is_supported_document(path) {
        return Err(IngestionError::InvalidInput(format!(
            "unsupported document extension .{extension}"
        )));
    }
    let title = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("Untitled document")
        .to_string();
    let mime_type = mime_guess::from_path(path)
        .first_raw()
        .unwrap_or("application/octet-stream")
        .to_string();
    let (parser, text) = match extension.as_str() {
        "pdf" => (
            "pdf-extract-0.12".to_string(),
            pdf_extract::extract_text_from_mem(bytes)
                .map_err(|error| IngestionError::Parse(error.to_string()))?,
        ),
        "docx" => (
            "openxml-text-v1".to_string(),
            extract_zip_xml(bytes, "word/document.xml")?,
        ),
        "odt" => (
            "opendocument-text-v1".to_string(),
            extract_zip_xml(bytes, "content.xml")?,
        ),
        "html" | "htm" => ("html-text-v1".to_string(), strip_html(&decode_text(bytes)?)),
        _ => ("utf8-text-v1".to_string(), decode_text(bytes)?),
    };
    let text = normalize_text(&text);
    if text.trim().is_empty() {
        return Err(IngestionError::Parse(
            "document did not contain extractable text".to_string(),
        ));
    }
    Ok(ExtractedDocument {
        title,
        mime_type,
        parser,
        text,
    })
}

fn extract_zip_xml(bytes: &[u8], member: &str) -> Result<String> {
    extract_zip_xml_with_limit(bytes, member, MAX_ARCHIVE_MEMBER_BYTES)
}

fn extract_zip_xml_with_limit(bytes: &[u8], member: &str, max_bytes: u64) -> Result<String> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|error| IngestionError::Parse(format!("invalid document archive: {error}")))?;
    let mut file = archive.by_name(member).map_err(|error| {
        IngestionError::Parse(format!("document archive is missing {member}: {error}"))
    })?;
    if file.size() > max_bytes {
        return Err(IngestionError::Parse(format!(
            "document archive member {member} exceeds the {max_bytes}-byte decompressed limit"
        )));
    }
    let mut xml = String::new();
    (&mut file)
        .take(max_bytes.saturating_add(1))
        .read_to_string(&mut xml)
        .map_err(|error| IngestionError::Parse(format!("invalid XML document: {error}")))?;
    if xml.len() as u64 > max_bytes {
        return Err(IngestionError::Parse(format!(
            "document archive member {member} exceeds the {max_bytes}-byte decompressed limit"
        )));
    }
    xml_to_text(&xml)
}

fn xml_to_text(xml: &str) -> Result<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut output = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Text(text)) => {
                let raw = String::from_utf8_lossy(text.as_ref());
                let unescaped = quick_xml::escape::unescape(&raw)
                    .map_err(|error| IngestionError::Parse(error.to_string()))?;
                output.push_str(&unescaped);
            }
            Ok(Event::CData(text)) => output.push_str(&String::from_utf8_lossy(text.as_ref())),
            Ok(Event::Start(start)) | Ok(Event::Empty(start)) => {
                let name = start.name();
                if matches!(name.as_ref(), b"w:tab" | b"text:tab") {
                    output.push('\t');
                } else if matches!(name.as_ref(), b"w:br" | b"text:line-break") {
                    output.push('\n');
                }
            }
            Ok(Event::End(end)) => {
                if matches!(end.name().as_ref(), b"w:p" | b"text:p" | b"text:h") {
                    output.push('\n');
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(IngestionError::Parse(error.to_string())),
        }
    }
    Ok(output)
}

fn decode_text(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|error| {
        IngestionError::Parse(format!(
            "text document must be UTF-8 (invalid byte at {})",
            error.utf8_error().valid_up_to()
        ))
    })
}

fn strip_html(html: &str) -> String {
    let mut output = String::with_capacity(html.len());
    let mut in_tag = false;
    for character in html.chars() {
        match character {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                output.push(' ');
            }
            _ if !in_tag => output.push(character),
            _ => {}
        }
    }
    output
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

fn normalize_text(text: &str) -> String {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut output = String::with_capacity(normalized.len());
    let mut blank_lines = 0_u8;
    for line in normalized.lines() {
        let line = line.trim_end();
        if line.trim().is_empty() {
            blank_lines = blank_lines.saturating_add(1);
            if blank_lines <= 1 {
                output.push('\n');
            }
        } else {
            blank_lines = 0;
            output.push_str(line);
            output.push('\n');
        }
    }
    output.trim().to_string()
}

fn extension(path: &Path) -> Option<String> {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use zip::{ZipWriter, write::SimpleFileOptions};

    use super::extract_zip_xml_with_limit;

    #[test]
    fn rejects_archive_members_over_the_decompressed_limit() {
        let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
        archive
            .start_file("word/document.xml", SimpleFileOptions::default())
            .unwrap();
        archive
            .write_all(b"<document>too large</document>")
            .unwrap();
        let bytes = archive.finish().unwrap().into_inner();

        let error = extract_zip_xml_with_limit(&bytes, "word/document.xml", 8).unwrap_err();
        assert!(error.to_string().contains("decompressed limit"));
    }
}
