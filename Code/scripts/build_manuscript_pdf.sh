#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
submission_root="$(cd "$repo_root/.." && pwd)"
package_dir="$submission_root/Final Report"
tex_file="Final Report.tex"

if [[ "${1:-main}" != "main" ]]; then
  echo "Only the final course-report PDF is included in this submission package." >&2
  exit 2
fi

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
