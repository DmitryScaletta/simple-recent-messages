use axum::{
    http::{
        HeaderMap, HeaderName, StatusCode,
        header::{CACHE_CONTROL, ETAG, IF_NONE_MATCH},
    },
    response::{IntoResponse, Response},
};

const RECENT_MESSAGES_CACHE_CONTROL: &str = "public, max-age=5, stale-while-revalidate=5";
const NO_STORE: &str = "no-store";

pub(super) fn etag(last_timestamp_ms: Option<i64>) -> String {
    match last_timestamp_ms {
        Some(timestamp_ms) => format!("W/\"ts-{timestamp_ms}\""),
        None => String::from("W/\"empty\""),
    }
}

pub(super) fn matches(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get_all(IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|candidate| candidate == "*" || opaque_tag(candidate) == opaque_tag(etag))
}

pub(super) fn headers(etag: &str) -> [(HeaderName, &str); 2] {
    [(CACHE_CONTROL, RECENT_MESSAGES_CACHE_CONTROL), (ETAG, etag)]
}

pub(super) fn not_modified(etag: &str) -> Response {
    (StatusCode::NOT_MODIFIED, headers(etag)).into_response()
}

pub(super) fn no_store(status: StatusCode) -> Response {
    (status, [(CACHE_CONTROL, NO_STORE)]).into_response()
}

fn opaque_tag(tag: &str) -> &str {
    tag.strip_prefix("W/").unwrap_or(tag)
}

#[cfg(test)]
mod tests {
    #![expect(clippy::expect_used, reason = "test assertions")]

    use axum::http::HeaderValue;

    use super::*;

    fn header_value(value: &str) -> HeaderValue {
        HeaderValue::from_str(value).expect("test header value should be valid")
    }

    #[test]
    fn etag_uses_last_message_timestamp() {
        assert_eq!(etag(None), "W/\"empty\"");
        assert_eq!(etag(Some(100)), "W/\"ts-100\"");
        assert_ne!(etag(Some(100)), etag(Some(200)));
    }

    #[test]
    fn if_none_match_matches_weak_tags_and_wildcards() {
        let tag = etag(Some(100));
        let strong = tag.strip_prefix("W/").expect("weak ETag");

        for candidate in ["*", tag.as_str(), strong] {
            let mut request_headers = HeaderMap::new();
            request_headers.insert(IF_NONE_MATCH, header_value(candidate));

            assert!(matches(&request_headers, &tag));
        }

        let mut request_headers = HeaderMap::new();
        request_headers.insert(IF_NONE_MATCH, header_value("W/\"other\""));

        assert!(!matches(&request_headers, &tag));
        assert!(!matches(&request_headers, &etag(None)));

        let mut wildcard_headers = HeaderMap::new();
        wildcard_headers.insert(IF_NONE_MATCH, header_value("*"));

        assert!(matches(&wildcard_headers, &etag(None)));
    }

    #[test]
    fn not_modified_keeps_cache_headers() {
        let response = not_modified("W/\"abc\"");

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(RECENT_MESSAGES_CACHE_CONTROL)
        );
        assert_eq!(
            response
                .headers()
                .get(ETAG)
                .and_then(|value| value.to_str().ok()),
            Some("W/\"abc\"")
        );
    }

    #[test]
    fn not_modified_with_empty_channel_etag_keeps_cache_headers() {
        let tag = etag(None);
        let response = not_modified(&tag);

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(RECENT_MESSAGES_CACHE_CONTROL)
        );
        assert_eq!(
            response
                .headers()
                .get(ETAG)
                .and_then(|value| value.to_str().ok()),
            Some("W/\"empty\"")
        );
    }

    #[test]
    fn errors_are_not_cached() {
        let response = no_store(StatusCode::BAD_REQUEST);

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(NO_STORE)
        );
        assert!(!response.headers().contains_key(ETAG));
    }
}
