# PDF Revision Workflow

For this project, edit the LaTeX source and rebuild the PDF. Do not edit the PDF binary as the main workflow.

## Files To Edit

- Main manuscript source:
  `Applied_Economics_Submission_Package (1)/Applied_Economics_Main_Manuscript.tex`
- Anonymous manuscript source:
  `Applied_Economics_Submission_Package (1)/Applied_Economics_Main_Manuscript_Anonymous.tex`
- Figure files:
  `Applied_Economics_Submission_Package (1)/figures/`

## Build The PDF

From the RADFM repository root:

```bash
bash scripts/build_manuscript_pdf.sh main
```

For the anonymous version:

```bash
bash scripts/build_manuscript_pdf.sh anonymous
```

This machine has `tectonic`, so a full TeX Live install is not required for this workflow.

## Recommended Review Process

1. Keep teacher or reviewer comments in the annotated PDF, a screenshot, or a short notes file.
2. Apply the real content changes in the `.tex` file.
3. Rebuild with `bash scripts/build_manuscript_pdf.sh main`.
4. Open the rebuilt PDF and check the changed pages.
5. If the anonymous version is needed, mirror the same edits and rebuild it too.

## Common Edits

- Text revisions: edit paragraphs directly in `.tex`.
- Tables: edit the `longtable` or `tabular` block in `.tex`.
- Figures: replace the source file in `figures/`, keeping the same filename if possible.
- Captions: edit `\caption{...}`.
- References: edit the `thebibliography` block.

## Notes

- Warnings such as `Underfull hbox` are usually layout warnings, not fatal compile errors.
- A missing figure or malformed LaTeX command is a real error and must be fixed before submission.
- Direct PDF edits are suitable only for temporary annotations. They are hard to track and can drift away from the LaTeX source.
