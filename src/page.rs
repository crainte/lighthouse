//! One bound on how much a list route will return.
//!
//! Every list endpoint in this project is read by something that renders it —
//! a status page, an admin table, the global dashboard's relay — and none of
//! them can usefully display more than a screenful at a time. The cap is not
//! there to protect the reader, though: it is there because `limit` arrives
//! from the network, the database is a single worker, and one request asking
//! for everything is enough to make every other request wait behind it.
//!
//! Defaults stay with the routes. A status page and an incident list disagree
//! about how many rows is a sensible first screen, and that is a presentation
//! question. The ceiling is not — a caller must not be able to opt out of it,
//! and three separate copies of "1,000" is three chances for one of them to
//! quietly become a different number.

/// Largest page any list route will return, whatever the caller asked for.
pub const MAX_PAGE_SIZE: usize = 1_000;

/// Resolve a requested page size against a route's own default.
pub fn bounded(requested: Option<usize>, default: usize) -> usize {
    requested.unwrap_or(default).min(MAX_PAGE_SIZE)
}

/// Reduce `items` to the `page`th window of `size`, reporting what the caller
/// cannot see: the total before paging, and whether anything follows.
///
/// Paging past the end empties the vector rather than failing. A reader paging
/// forward through a list that shrank underneath them has done nothing wrong,
/// and a 400 in that position reads as a bug in the client.
pub fn take<T>(items: &mut Vec<T>, page: usize, size: usize) -> (usize, bool) {
    let total = items.len();
    let start = page.saturating_mul(size).min(total);
    let end = start.saturating_add(size).min(total);
    let has_more = end < total;
    items.drain(..start);
    items.truncate(end - start);
    (total, has_more)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_requested_size_is_capped_but_a_smaller_one_is_honoured() {
        assert_eq!(bounded(None, 50), 50);
        assert_eq!(bounded(Some(20), 50), 20);
        assert_eq!(bounded(Some(MAX_PAGE_SIZE + 1), 50), MAX_PAGE_SIZE);
        assert_eq!(bounded(None, MAX_PAGE_SIZE * 2), MAX_PAGE_SIZE);
    }

    #[test]
    fn paging_past_the_end_is_empty_rather_than_an_error() {
        let page = |page, size| {
            let mut items = vec![1, 2, 3, 4, 5];
            let (total, has_more) = take(&mut items, page, size);
            (items, total, has_more)
        };
        assert_eq!(page(0, 2), (vec![1, 2], 5, true));
        assert_eq!(page(2, 2), (vec![5], 5, false));
        assert_eq!(page(9, 2), (vec![], 5, false));
        assert_eq!(page(usize::MAX, 10), (vec![], 5, false));
    }
}
