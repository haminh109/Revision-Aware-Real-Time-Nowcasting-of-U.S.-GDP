from pathlib import Path
from shutil import copyfile, rmtree
from tempfile import mkdtemp
import atexit

from fpdf import FPDF


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Final Project" / "Báo cáo dự án.md"
PDF_PATH = ROOT / "Final Project" / "Báo cáo dự án.pdf"

FONT_CACHE = Path(mkdtemp(prefix="project_executive_fonts_"))
atexit.register(lambda: rmtree(FONT_CACHE, ignore_errors=True))

SYSTEM_FONT_REGULAR = Path("/System/Library/Fonts/Supplemental/Times New Roman.ttf")
SYSTEM_FONT_BOLD = Path("/System/Library/Fonts/Supplemental/Times New Roman Bold.ttf")
SYSTEM_FONT_ITALIC = Path("/System/Library/Fonts/Supplemental/Times New Roman Italic.ttf")


def cached_font(source: Path) -> str:
    FONT_CACHE.mkdir(parents=True, exist_ok=True)
    target = FONT_CACHE / source.name
    if not target.exists():
        copyfile(source, target)
    return str(target)


class ExecutivePDF(FPDF):
    def footer(self):
        self.set_y(-12)
        self.set_font("TNR", "I", 8)
        self.set_text_color(95, 95, 95)
        self.cell(0, 6, str(self.page_no()), 0, 0, "C")


pdf = ExecutivePDF("P", "mm", "A4")
pdf.set_auto_page_break(True, margin=16)
pdf.set_margins(18, 14, 18)
pdf.add_font("TNR", "", cached_font(SYSTEM_FONT_REGULAR), uni=True)
pdf.add_font("TNR", "B", cached_font(SYSTEM_FONT_BOLD), uni=True)
pdf.add_font("TNR", "I", cached_font(SYSTEM_FONT_ITALIC), uni=True)

BODY_W = 174


def write_heading(text: str, level: int):
    if level == 1:
        pdf.set_font("TNR", "B", 16.2)
        pdf.set_text_color(20, 20, 20)
        pdf.multi_cell(BODY_W, 7.3, text)
        pdf.ln(1.6)
    elif level == 2:
        pdf.set_font("TNR", "B", 13)
        pdf.set_text_color(20, 20, 20)
        pdf.multi_cell(BODY_W, 6.1, text)
        pdf.ln(0.6)
    else:
        pdf.set_font("TNR", "B", 11.2)
        pdf.set_text_color(20, 20, 20)
        pdf.multi_cell(BODY_W, 5.5, text)
        pdf.ln(0.5)


def write_paragraph(text: str):
    pdf.set_font("TNR", "", 10.45)
    pdf.set_text_color(30, 30, 30)
    pdf.set_x(22)
    pdf.multi_cell(BODY_W - 4, 5.25, text)
    pdf.ln(1.5)


def wrap_text(text: str, width: float, font_size: float) -> list[str]:
    pdf.set_font("TNR", "", font_size)
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = word if not current else f"{current} {word}"
        if pdf.get_string_width(candidate) <= width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines or [""]


def parse_table_row(line: str) -> list[str]:
    return [part.strip() for part in line.strip().strip("|").split("|")]


def write_table(table_lines: list[str], caption: str | None = None):
    rows = [parse_table_row(line) for line in table_lines if not set(line.replace("|", "").strip()) <= {"-"}]
    if not rows:
        return

    widths = [42, 64, 68]
    x0 = 18
    line_h = 4.0
    font_size = 8.75
    header_font_size = 8.85
    cell_pad = 1.4

    wrapped_rows = []
    for row_index, row in enumerate(rows):
        row = row[:3] + [""] * max(0, 3 - len(row))
        wrapped = []
        for col_index, cell in enumerate(row[:3]):
            size = header_font_size if row_index == 0 else font_size
            wrapped.append(wrap_text(cell, widths[col_index] - 2 * cell_pad, size))
        height = max(len(lines) for lines in wrapped) * line_h + 2 * cell_pad
        wrapped_rows.append((wrapped, height))

    total_height = sum(height for _, height in wrapped_rows) + 10 + (6 if caption else 0)
    if pdf.get_y() + total_height > 281:
        pdf.add_page()

    pdf.ln(1.2)
    if caption:
        pdf.set_font("TNR", "", 10.2)
        pdf.set_text_color(20, 20, 20)
        pdf.set_x(x0)
        pdf.cell(sum(widths), 5.0, caption, 0, 1, "C")
        pdf.ln(0.2)

    y = pdf.get_y()
    pdf.set_draw_color(25, 25, 25)
    pdf.set_line_width(0.35)
    pdf.line(x0, y, x0 + sum(widths), y)
    y += 1.2

    for row_index, (wrapped, height) in enumerate(wrapped_rows):
        x = x0
        is_header = row_index == 0
        for col_index, lines in enumerate(wrapped):
            pdf.set_xy(x + cell_pad, y + cell_pad)
            pdf.set_font("TNR", "B" if is_header else "", header_font_size if is_header else font_size)
            pdf.set_text_color(30, 30, 30)
            for line in lines:
                pdf.cell(widths[col_index] - 2 * cell_pad, line_h, line, 0, 2)
            x += widths[col_index]
        y += height
        if is_header:
            pdf.set_draw_color(80, 80, 80)
            pdf.set_line_width(0.22)
            pdf.line(x0, y, x0 + sum(widths), y)
            y += 0.8
    pdf.set_draw_color(25, 25, 25)
    pdf.set_line_width(0.35)
    pdf.line(x0, y, x0 + sum(widths), y)
    pdf.set_y(y + 2.5)


def render_markdown():
    pdf.add_page()
    pending: list[str] = []
    table_caption: str | None = None
    lines = SOURCE.read_text(encoding="utf-8").splitlines()
    i = 0
    while i < len(lines):
        raw_line = lines[i]
        line = raw_line.strip()
        if not line:
            if pending:
                write_paragraph(" ".join(pending))
                pending = []
            i += 1
            continue
        if line.startswith("Table ") and ":" in line:
            if pending:
                write_paragraph(" ".join(pending))
                pending = []
            table_caption = line
            i += 1
            continue
        if line.startswith("|"):
            if pending:
                write_paragraph(" ".join(pending))
                pending = []
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i].strip())
                i += 1
            write_table(table_lines, table_caption)
            table_caption = None
            continue
        if line.startswith("#"):
            if pending:
                write_paragraph(" ".join(pending))
                pending = []
            level = len(line) - len(line.lstrip("#"))
            write_heading(line[level:].strip(), level)
        else:
            pending.append(line)
        i += 1
    if pending:
        write_paragraph(" ".join(pending))


render_markdown()
PDF_PATH.parent.mkdir(parents=True, exist_ok=True)
pdf.output(str(PDF_PATH))
print(PDF_PATH)
