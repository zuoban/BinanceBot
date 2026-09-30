use std::sync::LazyLock;

pub static INDEX_HTML: LazyLock<String> = LazyLock::new(|| {
    include_str!("dashboard.html").replace("{{APP_VERSION}}", env!("CARGO_PKG_VERSION"))
});
