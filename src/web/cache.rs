use axum::{
    http::{
        HeaderMap, HeaderName, StatusCode,
        header::{CACHE_CONTROL, ETAG, IF_NONE_MATCH},
    },
    response::{IntoResponse, Response},
};

const RECENT_MESSAGES_CACHE_CONTROL: &str = "public, max-age=5, stale-while-revalidate=5";
const NO_STORE: &str = "no-store";

pub(super) fn create_etag(last_timestamp_ms: Option<i64>) -> String {
    match last_timestamp_ms {
        Some(timestamp_ms) => format!("W/\"{timestamp_ms}\""),
        None => String::from("W/\"empty\""),
    }
}

pub(super) fn parse_etag_timestamp(headers: &HeaderMap) -> Option<i64> {
    headers
        .get_all(IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|tag| {
            let tag = tag.trim();
            let tag = tag.strip_prefix("W/").unwrap_or(tag);
            tag.strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
                .and_then(|timestamp| timestamp.parse().ok())
        })
        .max()
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
        assert_eq!(create_etag(None), "W/\"empty\"");
        assert_eq!(create_etag(Some(100)), "W/\"100\"");
        assert_ne!(create_etag(Some(100)), create_etag(Some(200)));
    }

    #[test]
    fn parses_largest_timestamp_from_if_none_match() {
        let mut request_headers = HeaderMap::new();
        request_headers.insert(IF_NONE_MATCH, header_value("W/\"100\", \"200\""));

        assert_eq!(parse_etag_timestamp(&request_headers), Some(200));

        request_headers.insert(IF_NONE_MATCH, header_value("W/\"100\""));

        assert_eq!(parse_etag_timestamp(&request_headers), Some(100));

        request_headers.insert(IF_NONE_MATCH, header_value("\"100\""));

        assert_eq!(parse_etag_timestamp(&request_headers), Some(100));
    }

    #[test]
    fn ignores_wildcard_empty_and_unknown_tags() {
        for candidate in ["*", "W/\"empty\"", "W/\"other\"", "W/\"ts-100\""] {
            let mut request_headers = HeaderMap::new();
            request_headers.insert(IF_NONE_MATCH, header_value(candidate));

            assert_eq!(parse_etag_timestamp(&request_headers), None);
        }

        assert_eq!(parse_etag_timestamp(&HeaderMap::new()), None);
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
        let tag = create_etag(None);
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
