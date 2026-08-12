#!/usr/bin/env bash
# Build all datasource + ETL plugins and copy the cdylibs into
# DataTransfer/plugins so config.yaml finds them.
set -euo pipefail

cd "$(dirname "$0")"

cargo build --workspace

TARGET=DataTransfer/plugins
mkdir -p "$TARGET/datasource" "$TARGET/etl"

# Datasource plugins (cdylib crate names are driver libs).
copy_plugin() {
    local crate="$1" name="$2"
    local src="target/debug/lib${name}.so"
    if [[ -f "$src" ]]; then
        cp -f "$src" "$TARGET/datasource/lib${name}.so"
        echo "  -> plugins/datasource/lib${name}.so"
    else
        echo "warning: $crate produced no $src" >&2
    fi
}

copy_plugin MysqlPlugin mysql
copy_plugin PostgresqlPlugin postgresql
copy_plugin OraclePlugin oracle

# ETL plugins (cdylib crates under LibETL/).
copy_etl_plugin() {
    local crate="$1" name="$2"
    local src="target/debug/lib${name}.so"
    if [[ -f "$src" ]]; then
        cp -f "$src" "$TARGET/etl/lib${name}.so"
        echo "  -> plugins/etl/lib${name}.so"
    else
        echo "warning: $crate produced no $src" >&2
    fi
}

copy_etl_plugin OrderSplitPlugin order_split

echo "Build complete. Plugins are under $TARGET"
