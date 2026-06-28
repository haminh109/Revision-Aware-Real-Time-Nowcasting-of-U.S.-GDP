#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package_dir="$repo_root/Applied_Economics_Submission_Package (1)"

target="${1:-main}"
case "$target" in
  main)
    tex_file="Applied_Economics_Main_Manuscript.tex"
    ;;
  anonymous|anon)
    tex_file="Applied_Economics_Main_Manuscript_Anonymous.tex"
    ;;
  *)
    echo "Usage: $0 [main|anonymous]" >&2
    exit 2
    ;;
esac

if ! command -v tectonic >/dev/null 2>&1; then
  echo "tectonic is required but was not found in PATH." >&2
  exit 1
fi

if [[ ! -f "$package_dir/$tex_file" ]]; then
  echo "Cannot find LaTeX source: $package_dir/$tex_file" >&2
  exit 1
fi

cd "$package_dir"
tectonic "$tex_file"
echo "Built: $package_dir/${tex_file%.tex}.pdf"
