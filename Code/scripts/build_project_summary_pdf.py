from pathlib import Path
from shutil import copyfile, rmtree
from tempfile import mkdtemp
import atexit

from fpdf import FPDF


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "Final Project"
PDF_PATH = OUT_DIR / "Project_Summary_2_Pages.pdf"

FONT_CACHE = Path(mkdtemp(prefix="project_summary_fonts_"))
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


class SummaryPDF(FPDF):
    def footer(self):
        self.set_y(-11)
        self.set_font("TNR", "I", 8)
        self.set_text_color(95, 95, 95)
        self.cell(0, 6, f"RADFM GDP nowcasting project summary - page {self.page_no()}/2", 0, 0, "C")


pdf = SummaryPDF("P", "mm", "A4")
pdf.set_auto_page_break(False)
pdf.set_margins(16, 11, 16)
pdf.add_font("TNR", "", cached_font(SYSTEM_FONT_REGULAR), uni=True)
pdf.add_font("TNR", "B", cached_font(SYSTEM_FONT_BOLD), uni=True)
pdf.add_font("TNR", "I", cached_font(SYSTEM_FONT_ITALIC), uni=True)

LEFT = 16
WIDTH = 178


def title_page_header():
    pdf.set_text_color(20, 20, 20)
    pdf.set_font("TNR", "B", 15)
    pdf.set_xy(LEFT, 11)
    pdf.multi_cell(WIDTH, 6.2, "Báo cáo tóm tắt 2 trang: Release-Ladder Real-Time Nowcasting of U.S. GDP", 0, "L")
    pdf.set_font("TNR", "", 9.2)
    pdf.set_text_color(80, 80, 80)
    pdf.set_xy(LEFT, 25)
    pdf.cell(WIDTH, 5, "Dự án RADFM - Time Series Final Project - local report, không push Git", 0, 1)
    pdf.set_draw_color(160, 160, 160)
    pdf.line(LEFT, 32, LEFT + WIDTH, 32)
    return 36


def page_header(text):
    pdf.set_text_color(20, 20, 20)
    pdf.set_font("TNR", "B", 14)
    pdf.set_xy(LEFT, 12)
    pdf.cell(WIDTH, 6, text, 0, 1)
    pdf.set_draw_color(160, 160, 160)
    pdf.line(LEFT, 21, LEFT + WIDTH, 21)
    return 26


def section_heading(text, y):
    pdf.set_xy(LEFT, y)
    pdf.set_font("TNR", "B", 10.6)
    pdf.set_text_color(31, 78, 121)
    pdf.cell(WIDTH, 5.4, text, 0, 1)
    return pdf.get_y() + 1.0


def paragraph(text, y, size=9.15, line_h=4.45, style="", indent=True):
    pdf.set_xy(LEFT + (4 if indent else 0), y)
    pdf.set_font("TNR", style, size)
    pdf.set_text_color(30, 30, 30)
    pdf.multi_cell(WIDTH - (4 if indent else 0), line_h, text)
    return pdf.get_y() + 1.4


def compact_paragraph(text, y, style="", indent=True):
    return paragraph(text, y, size=8.95, line_h=4.2, style=style, indent=indent)


def emphasized(text, y):
    pdf.set_xy(LEFT, y)
    pdf.set_font("TNR", "B", 9.2)
    pdf.set_text_color(95, 70, 20)
    pdf.multi_cell(WIDTH, 4.2, text)
    return pdf.get_y() + 1.6


# Page 1
pdf.add_page()
y = title_page_header()

y = section_heading("1. Đề tài nghiên cứu", y)
y = paragraph(
    "Dự án nghiên cứu bài toán nowcasting GDP thực của Hoa Kỳ trong bối cảnh GDP được công bố theo nhiều vòng: advance, second, third và mature release. Thay vì coi nowcasting là bài toán dự báo một chuỗi GDP final-vintage duy nhất, nghiên cứu xem mỗi thời điểm công bố là một bài toán forecast riêng với target, information set và benchmark riêng.",
    y,
)
y = paragraph(
    "Điểm trung tâm là release ladder. Trước advance release, người dự báo chưa có GDP chính thức của quý hiện tại. Trước second release, advance estimate đã công khai. Trước third release, second estimate đã công khai. Vì vậy, cùng là GDP nowcasting nhưng câu hỏi kinh tế và tiêu chuẩn đánh giá thay đổi theo thời điểm ra quyết định.",
    y,
)

y = section_heading("2. Mục lục và nội dung các phần trong bài", y + 0.5)
y = compact_paragraph(
    "Phần Introduction nêu vấn đề dự báo GDP theo thời gian thực và giải thích vì sao release timing cùng data revisions làm thay đổi bài toán nowcasting. Phần Literature Review đặt nghiên cứu trong các nhánh nowcasting, dynamic factor models, mixed-frequency methods, real-time vintages, GDP revisions và density forecast evaluation.",
    y,
)
y = compact_paragraph(
    "Phần Data and Methodology xây dựng các advance, second, third và mature targets; căn chỉnh ALFRED/FRED vintages, BEA release dates, SPF forecasts và benchmark no-revision theo từng checkpoint. Phần Econometric Framework trình bày các benchmark như SPF, no-revision, AR, bridge, MIDAS/UMIDAS, DFM và release-revision state-space models dùng Kalman filtering và EM.",
    y,
)
y = compact_paragraph(
    "Phần Results and Discussion báo cáo point forecasts, density forecasts, revision-risk diagnostics, exact-versus-pseudo timing và inference bằng block bootstrap. Phần Conclusion and Limitations kết luận rằng benchmark phải khớp với release stage; state-space models hữu ích nhất cho phân phối dự báo và revision risk, không phải lúc nào cũng thắng point RMSE.",
    y,
)

y = section_heading("3. Thiết kế nghiên cứu", y + 0.5)
y = paragraph(
    "Dữ liệu được xây dựng theo exact-vintage discipline cho giai đoạn 2005Q1-2024Q4. Mỗi forecast chỉ sử dụng thông tin thật sự có sẵn tại forecast origin: monthly indicators, historical GDP releases, GDP release calendar và SPF forecasts nếu timing tương thích. Cách làm này ngăn look-ahead bias, tức không cho mô hình sử dụng thông tin mà người dự báo thực tế chưa quan sát được.",
    y,
)
y = paragraph(
    "Thiết kế train-test là expanding-window recursive forecasting với minimum training length 48 quarters. Các mô hình được đánh giá bằng RMSE, MAE, bias, CRPS, interval coverage, revision-risk scores và model-confidence-set-style diagnostics với 5,000 moving-block bootstrap replications. Mục tiêu là đánh giá cả point accuracy lẫn uncertainty quantification, thay vì chỉ chọn mô hình có RMSE thấp nhất.",
    y,
)
y = emphasized(
    "Thông điệp phương pháp: không đánh giá mọi forecast bằng một final-vintage GDP target. Người dùng thực tế luôn đứng tại một release checkpoint cụ thể, nên benchmark đúng cũng phải thay đổi theo checkpoint đó.",
    y + 1.0,
)


# Page 2
pdf.add_page()
y = page_header("Kết quả nghiên cứu và giá trị ứng dụng")

y = section_heading("4. Kết quả nghiên cứu chính", y)
y = paragraph(
    "Kết quả point forecast cho thấy tính phụ thuộc vào release stage rất rõ. Trước advance release, SPF là benchmark điểm mạnh nhất với exact-timing RMSE 2.225. Điều này cho thấy trước khi GDP chính thức được công bố, professional forecasts vẫn tổng hợp tốt thông tin vĩ mô mà các mô hình monthly-indicator trong bài chưa khai thác hết.",
    y,
)
y = paragraph(
    "Sau khi official GDP estimate đã công khai, bài toán thay đổi đáng kể. Trước second release, no-revision là benchmark mạnh nhất với RMSE 0.570; trước third release, no-revision tiếp tục dẫn đầu với RMSE 0.362. Khi advance hoặc second estimate đã có, dự báo release tiếp theo thực chất là một bài toán revision forecasting, và official estimate hiện tại đã là một point predictor rất mạnh.",
    y,
)
y = paragraph(
    "Release-revision state-space models không tạo ra point-RMSE dominance phổ quát, nhưng đóng góp rõ nhất ở density forecasts và revision-risk assessment. Indicator-revision SSM có CRPS thấp nhất cho third-release point density, khoảng 0.187 so với no-revision 0.198. Với second-to-third revision density, GDP-revision SSM giảm CRPS xuống khoảng 0.184 so với no-revision 0.197.",
    y,
)

y = section_heading("5. Tính ứng dụng và thực tiễn", y + 0.5)
y = paragraph(
    "Dự án có giá trị thực tiễn vì nó trả lời đúng câu hỏi mà người dùng vĩ mô cần biết: tại mỗi thời điểm công bố GDP, benchmark nào là hợp lý và mô hình nên đóng vai trò gì? Với policy analysts, release-ladder evaluation giúp tránh đánh giá sai mô hình bằng cách so sánh forecast real-time với final-vintage GDP không có thật tại thời điểm quyết định.",
    y,
)
y = paragraph(
    "Với market analysts và forecasting desks, kết quả khuyến nghị báo cáo forecast theo checkpoint: pre-advance, pre-second và pre-third, thay vì một headline nowcast duy nhất. Sau advance release, người dùng không chỉ cần một con số point forecast; họ cần biết xác suất revision lớn, khả năng revision âm và mức uncertainty quanh official estimate hiện tại.",
    y,
)
y = paragraph(
    "Thông điệp quản trị mô hình cũng rất rõ. Trước advance release, mô hình cần được so với SPF. Sau advance release, mô hình phải được so với no-revision. Nếu mô hình không thắng point RMSE, nó vẫn có giá trị thực tế nếu cung cấp xác suất, prediction intervals và revision-risk distribution tốt hơn cho quá trình ra quyết định.",
    y,
)

y = section_heading("6. Kết luận ngắn", y + 0.5)
y = paragraph(
    "Nghiên cứu không tuyên bố rằng state-space models luôn vượt trội. Kết luận quan trọng hơn là GDP nowcasting phải được đánh giá theo release stage. Official early GDP releases là point benchmarks rất mạnh; release-aware state-space structure hữu ích nhất khi người dùng cần đo lường uncertainty, predictive intervals và revision-risk distribution quanh các official estimates đó.",
    y,
)
y = emphasized(
    "Quy tắc sử dụng thực tế: trước advance release, benchmark chính là SPF; sau advance release, benchmark chính là no-revision. Mô hình có thể không thắng RMSE nhưng vẫn có giá trị nếu nó định lượng tốt rủi ro revision.",
    y + 1.0,
)

PDF_PATH.parent.mkdir(parents=True, exist_ok=True)
pdf.output(str(PDF_PATH))
print(PDF_PATH)
