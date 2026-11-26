//! Stable display vocabulary and name derivation for Run agents.

use sha2::{Digest, Sha256};

// Vocabulary and index order copied from Agent Bridge framing/names.rs.

/// First words in stable index order. Reordering changes existing display names.
const ADJECTIVES: &str = "\
Amber Azure Alpine Analog Arctic Astral Auburn Autumn Avid Balsam
Baltic Beaded Blooming Blue Bold Boreal Breezy Bright Brisk Bronze
Brown Brushed Calm Celadon Centered Cherry Chilled Chiming Circular Classic
Clear Coastal Cobalt Compact Cool Copper Coral Cosmic Crisp Curved
Dappled Deep Delicate Dewy Digital Distant Dry Dusky Early Earthy
Eastern Edged Elastic Emerald Even Faint Fine Floral Flowing Foggy
Folded Fractal Fresh Frosted Frozen Fuzzy Gentle Gilded Glassy Glossy
Golden Grassy Green Guided Hazel Hazy Hidden Icy Inner Ivory
Jade Keen Khaki Kind Laced Leafy Level Light Lilac Linear
Lively Local Lofty Long Lunar Lush Lustrous Marble Marine Matte
Mellow Misty Modest Mosaic Mossy Muted Narrow Near Neat Nimble
Noble Northern Ochre Olive Onyx Opal Open Orange Orchid Outer
Pacific Pale Pastel Peach Pearly Pebbled Pink Plain Placid Plum
Plush Polished Quiet Rapid Remote Rich Ridged Rippled Rocky Rolling
Rosy Round Ruby Rusty Sandy Satin Scarlet Shaded Shiny Silky
Silver Simple Slate Sleek Slim Smooth Snowy Soft Solar Solid
Southern Spherical Spotted Spring Starlit Steady Steel Stellar Still Striped
Subtle Summer Sunny Swift Tawny Teal Tender Thin Tidal Tinted
Topaz Traced Trim Umber Velvet Vivid Warm Wavy Western White
Wild Willow Windy Winter Wispy Woven Yellow Young Zesty";

/// Second words in stable index order. Reordering changes existing display names.
const NOUNS: &str = "\
Acorn Alder Alpaca Anchor Apricot Arbor Arch Aspen Aster Avocet
Badger Barn Basin Bay Beacon Bee Beech Bell Bison Bloom
Boulder Bramble Branch Breeze Brook Buffalo Cabin Cairn Canary Candle
Canoe Canyon Carp Cat Cave Chime Circle Cliff Cloud Clover
Coast Comet Cone Cove Crane Creek Crest Cricket Crocus Cube
Cypress Dahlia Daisy Deer Delta Dew Dome Dove Drum Dune
Eagle Echo Elm Falcon Fennel Finch Fir Fjord Flame Flute
Foal Fog Fox Frond Frost Gable Gale Garden Gate Gazelle
Geode Glade Globe Grove Gull Harbor Hare Harp Hawk Heron
Hill Hive Holly Horizon Iris Isle Ivy Jay Juniper Kestrel
Kite Knoll Lagoon Lake Larch Lark Laurel Leaf Lens Lichen
Lily Llama Magpie Maple Marsh Meadow Mesa Minnow Moon Moss
Moth Nest Newt Oak Oat Osprey Otter Owl Palm Panda
Park Path Pearl Pebble Petal Pine Plover Pond Poppy Porch
Prism Quail Quartz Quill Rabbit Rain Raven Reed Reef Ridge
Robin Rock Root Rowan Sail Sand Seal Seed Shade Shell
Shore Shrub Siskin Skiff Sky Slope Snow Sparrow Sprig Sprout
Stag Star Stem Stilt Stoat Stone Stork Stream Summit Swan
Swift Tern Thyme Tide Trout Tulip Vale Vetch Vine Vista
Wave Web Wheat Wing Wren Yak Yew Zebra Zinnia";

/// Derives a stable display name from the lineage root ID using Bridge vocabulary.
pub fn agent_name(root_agent_id: &str) -> String {
    let digest = Sha256::digest(root_agent_id.as_bytes());
    name_from_digest((&digest[..8]).try_into().expect("SHA-256 has 32 bytes"))
}

/// Maps the leading eight bytes of a root agent ID digest to a display name.
fn name_from_digest(digest: [u8; 8]) -> String {
    let [a, b, c, d, e, f, g, h] = digest;
    let adjective = word_at(ADJECTIVES, u32::from_be_bytes([a, b, c, d]));
    let noun = word_at(NOUNS, u32::from_be_bytes([e, f, g, h]));
    format!("{adjective} {noun}")
}

/// Selects one word by reducing an index modulo the vocabulary length.
fn word_at(words: &str, index: u32) -> &str {
    let count = words.split_ascii_whitespace().count();
    words
        .split_ascii_whitespace()
        .nth(index as usize % count)
        .expect("nonempty fixed vocabulary")
}

/// Formats a display name and profile beside the stable ID for plain-text notices.
pub fn agent_label(name: &str, profile: &str, agent_id: &str) -> String {
    format!(
        "{} ({}) · {agent_id}",
        single_line(name),
        single_line(profile)
    )
}

/// Escapes line breaks, controls and bidi formatting in display metadata.
fn single_line(value: &str) -> String {
    let mut output = String::new();
    for ch in value.chars() {
        if ch.is_control()
            || matches!(ch, '\u{2028}' | '\u{2029}' | '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        {
            output.extend(ch.escape_unicode());
        } else {
            output.push(ch);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins three Bridge fixture inputs and a Run lineage root.
    #[test]
    fn matches_bridge_names() {
        assert_eq!(agent_name("example/claude/default/session"), "Shaded Snow");
        assert_eq!(agent_name("example/codex/default/session"), "Elastic Glade");
        assert_eq!(agent_name("codex/session"), "Zesty Petal");
        assert_eq!(agent_name("ag-20260825-120000-0123456789"), "Delicate Fox");
    }

    /// Keeps notice labels on one line and exposes bidi controls.
    #[test]
    fn labels_escape_invisible_formatting() {
        assert_eq!(
            agent_label("A\nB", "role\u{202e}", "ag-1"),
            r"A\u{a}B (role\u{202e}) · ag-1"
        );
    }
}
