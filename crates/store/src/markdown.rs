//! The markdown codec, slice 1 edition.
//!
//! A document is a flat sequence of top-level blocks. Each block keeps the
//! exact source text it came from, so rendering is concatenation and the round
//! trip is lossless up to the blank lines between blocks. A real tree (nested
//! lists, sections) comes later; flat blocks are enough to hang IDs, versions
//! and blame on.

use pulldown_cmark::{Event, Options, Parser, Tag};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawBlock {
    pub kind: String,
    pub body: String,
}

pub fn split(source: &str) -> Vec<RawBlock> {
    let mut blocks = Vec::new();
    let mut depth = 0usize;
    for (event, range) in Parser::new_ext(source, Options::all()).into_offset_iter() {
        match event {
            Event::Start(tag) => {
                if depth == 0 {
                    blocks.push(RawBlock {
                        kind: kind_of(&tag).to_owned(),
                        body: source[range].trim_end().to_owned(),
                    });
                }
                depth += 1;
            }
            Event::End(_) => depth -= 1,
            // Leaf blocks with no Start/End pair.
            Event::Rule if depth == 0 => blocks.push(RawBlock {
                kind: "rule".to_owned(),
                body: source[range].trim_end().to_owned(),
            }),
            Event::Html(_) if depth == 0 => blocks.push(RawBlock {
                kind: "html".to_owned(),
                body: source[range].trim_end().to_owned(),
            }),
            _ => {}
        }
    }
    blocks
}

pub fn render<'a>(bodies: impl IntoIterator<Item = &'a str>) -> String {
    let mut out = bodies.into_iter().collect::<Vec<_>>().join("\n\n");
    out.push('\n');
    out
}

/// A proposal edits one block at a time, so its text has to parse as exactly
/// one block. Anything else would silently merge or split blocks and break the
/// IDs that history and blame hang on.
pub fn single_block(source: &str) -> Option<RawBlock> {
    let mut blocks = split(source);
    (blocks.len() == 1).then(|| blocks.remove(0))
}

fn kind_of(tag: &Tag) -> &'static str {
    match tag {
        Tag::Heading { .. } => "heading",
        Tag::Paragraph => "paragraph",
        Tag::BlockQuote(_) => "quote",
        Tag::CodeBlock(_) => "code",
        Tag::List(_) => "list",
        Tag::Table(_) => "table",
        Tag::HtmlBlock => "html",
        Tag::MetadataBlock(_) => "frontmatter",
        Tag::FootnoteDefinition(_) => "footnote",
        Tag::DefinitionList => "definitions",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &str = "\
---
title: Cell
---

# High-pressure cell

The seal is rated for 200 bar.

- copper gasket
- torque 12 Nm

| part | qty |
| ---- | --- |
| bolt | 8   |

```rust
let x = 1;
```

---

> check before each run
";

    #[test]
    fn splits_top_level_blocks() {
        let kinds: Vec<_> = split(DOC).into_iter().map(|b| b.kind).collect();
        assert_eq!(
            kinds,
            [
                "frontmatter",
                "heading",
                "paragraph",
                "list",
                "table",
                "code",
                "rule",
                "quote"
            ]
        );
    }

    #[test]
    fn round_trip_is_lossless() {
        let blocks = split(DOC);
        assert_eq!(render(blocks.iter().map(|b| b.body.as_str())), DOC);
    }

    #[test]
    fn single_block_rejects_two() {
        assert!(single_block("one\n\ntwo").is_none());
        assert_eq!(single_block("## hi").unwrap().kind, "heading");
    }
}
