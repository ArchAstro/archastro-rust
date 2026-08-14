#!/usr/bin/env bash
# Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
# Regenerate typed Rust sources and contract tests from the canonical spec.

set -euo pipefail
cd "$(dirname "$0")/.."

spec_dest="specs/platform-openapi.json"
config="scripts/sdk-generator-config.json"

if [[ "${1:-}" == "--local" ]]; then
  source_root="${2:?usage: regenerate_sdk.sh --local <archastro-openapi-checkout>}"
  cp "$source_root/specs/platform-openapi.json" "$spec_dest"
  generator="${ARCHASTRO_SDK_GENERATOR_BIN:-$source_root/packages/sdk-generator/dist/index.js}"
else
  ref="${ARCHASTRO_OPENAPI_REF:-main}"
  curl -fsSL "https://raw.githubusercontent.com/ArchAstro/archastro-openapi/$ref/specs/platform-openapi.json" -o "$spec_dest"
  generator="${ARCHASTRO_SDK_GENERATOR_BIN:-node_modules/.bin/sdk-generator}"
fi

node "$generator" --spec "$spec_dest" --config "$config" --lang rust --out .
node "$generator" --spec "$spec_dest" --config "$config" --lang contract-tests-rust --out .
cargo fmt --all
echo "Generated Rust SDK and contract tests."

