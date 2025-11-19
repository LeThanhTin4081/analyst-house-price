import streamlit as st
import pandas as pd
import joblib
import re
import numpy as np
import os

# ==== THÔNG TIN CHUNG VỀ DATASET (bạn chỉnh lại nếu cần) ====
DATA_LAST_UPDATED = "08/2024"   # Dữ liệu cập nhật đến tháng / năm nào


# --- 1. CLASS DỰ BÁO (giống logic khi train) ---
class RealEstatePredictor:
    def __init__(self):
        # --- ĐOẠN CODE SỬA LỖI ĐƯỜNG DẪN ---
        # Lấy vị trí thực tế của file app.py đang đứng
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Chỉ đường dẫn cụ thể tới các file .pkl nằm cùng chỗ với app.py
        model_path = os.path.join(current_dir, 'model.pkl')
        cols_path = os.path.join(current_dir, 'model_columns.pkl')
        ref_path = os.path.join(current_dir, 'market_reference_data.pkl')

        # Load model bằng đường dẫn tuyệt đối (không sợ lỗi nữa)
        self.model = joblib.load(model_path)
        self.model_columns = joblib.load(cols_path)
        self.market_ref = joblib.load(ref_path)

    def predict(self, quan_huyen, loai_bds, dien_tich, mat_tien, lai_suat_hien_tai=None):
        # Lấy thông tin thị trường quá khứ
        try:
            market_stats = self.market_ref.loc[(quan_huyen, loai_bds)]
        except KeyError:
            return None  # Không tìm thấy khu vực này

        # Lãi suất: nếu user không nhập thì lấy mặc định từ data
        lai_suat = lai_suat_hien_tai if lai_suat_hien_tai is not None else market_stats['LaiSuat_LNH_QuaDem']

        # TÍNH TOÁN FEATURE TƯƠNG TÁC (giống lúc train)
        impact_dt = lai_suat * market_stats['DienTich_TB_Ngay_Lag7']
        impact_gia = lai_suat * market_stats['Gia_TB_Ngay_Log']
        vithe_gia = market_stats['Gia_TB_Ngay_Log'] - market_stats['Gia_KhuVuc_Lag7']

        # Tạo input data
        input_data = {
            'LaiSuat_LNH_QuaDem': lai_suat,
            'SoLuong_Tin_Ngay_Lag7': market_stats['SoLuong_Tin_Ngay_Lag7'],
            'TocDo_ThayDoi_Volume': market_stats['TocDo_ThayDoi_Volume'],
            'DienTich_TB_Ngay_Lag7': market_stats['DienTich_TB_Ngay_Lag7'],
            'TyLe_MatTien_Ngay_Lag7': market_stats['TyLe_MatTien_Ngay_Lag7'],
            'Gia_KhuVuc_Lag7': market_stats['Gia_KhuVuc_Lag7'],
            'Gia_KhuVuc_Lag30': market_stats['Gia_KhuVuc_Lag30'],
            'Impact_LaiSuat_DienTich': impact_dt,
            'Impact_LaiSuat_Gia': impact_gia,
            'ViThe_Gia': vithe_gia
        }

        # Chuẩn khung input theo model_columns
        df_final_input = pd.DataFrame(columns=self.model_columns)
        df_final_input.loc[0] = 0.0

        # Điền dữ liệu số
        for col, val in input_data.items():
            if col in df_final_input.columns:
                df_final_input.at[0, col] = val

        # One-Hot Encoding cho quận/huyện & loại BĐS
        def clean_name(name):
            return re.sub('[^A-Za-z0-9_]+', '', name)

        col_quan = clean_name(f"QuanHuyen_{quan_huyen}")
        col_loai = clean_name(f"LoaiBDS_{loai_bds}")

        if col_quan in df_final_input.columns:
            df_final_input.at[0, col_quan] = 1
        if col_loai in df_final_input.columns:
            df_final_input.at[0, col_loai] = 1

        # Dự báo
        prediction = self.model.predict(df_final_input)[0]
        probability = self.model.predict_proba(df_final_input)[0][1]

        return prediction, probability, vithe_gia


# --- 2. CẤU HÌNH TRANG & CSS ---
st.set_page_config(
    page_title="Bất Động Sản AI",
    page_icon="🏠",
    layout="wide"
)

st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    .big-title {
        font-size: 40px;
        font-weight: 800;
        margin-bottom: 0.2rem;
    }
    .subtitle {
        font-size: 16px;
        color: #cccccc;
    }

    .stButton>button {
        border-radius: 999px;
        padding: 0.6em 2.5em;
        font-weight: 600;
    }

    .result-card {
        background: #111827;
        border-radius: 1rem;
        border: 1px solid #374151;
        padding: 1.2rem 1.5rem;
        margin-top: 0.5rem;
        margin-bottom: 0.8rem;
    }
    .scenario-text {
        font-size: 0.9rem;
        color: #d1d5db;
        margin-top: 0.2rem;
    }

    .badge-up {
        display: inline-block;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        background: #065f46;
        color: #d1fae5;
        font-size: 0.75rem;
        font-weight: 600;
        margin-top: 0.25rem;
    }
    .badge-down {
        display: inline-block;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        background: #7f1d1d;
        color: #fee2e2;
        font-size: 0.75rem;
        font-weight: 600;
        margin-top: 0.25rem;
    }

    .small-footer {
        font-size: 0.7rem;
        color: #6b7280;
        margin-top: 1.5rem;
        text-align: right;
    }
    </style>
""", unsafe_allow_html=True)

# Tiêu đề
st.markdown('<div class="big-title">Bất Động Sản AI</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">'
    'Công cụ dùng trí tuệ nhân tạo để <b>dự đoán xu hướng giá nhà đất tại TP.HCM</b> '
    'trong <b>khoảng 7 ngày tới</b>, dựa trên dữ liệu 7–30 ngày gần nhất.'
    '</div>',
    unsafe_allow_html=True
)
st.write("")


# --- 3. CACHE MODEL ---
@st.cache_resource
def load_predictor():
    return RealEstatePredictor()


# --- 4. APP CHÍNH ---
try:
    bot = load_predictor()

    all_locations = bot.market_ref.index.get_level_values(0).unique().tolist()
    all_types = bot.market_ref.index.get_level_values(1).unique().tolist()

    # SIDEBAR
    with st.sidebar:
        st.header("ℹ️ Công cụ này làm gì?")
        st.markdown(f"""
        **Bất Động Sản AI** giúp bạn:
        - Xem **xu hướng giá** (tăng hay đi ngang/giảm) của từng khu vực tại TP.HCM.  
        - Dự báo trong **khoảng 1 tuần tới**, dựa trên dữ liệu lịch sử 7–30 ngày gần nhất.  

        **Dữ liệu dùng để huấn luyện mô hình** được thu thập và xử lý đến: **{DATA_LAST_UPDATED}**.

        **Dữ liệu đầu vào** gồm:
        - Quận / huyện.  
        - Loại bất động sản (nhà phố, đất, căn hộ, ...).  
        - Diện tích và thông tin có mặt tiền đường hay không.  
        - Lãi suất điều hành của Ngân hàng Nhà nước (đã có sẵn giá trị gợi ý).  
        
        **Lưu ý:**
        - Kết quả chỉ mang tính **tham khảo**, không phải khuyến nghị đầu tư.  
        - Công cụ không xem xét các yếu tố pháp lý, quy hoạch chi tiết, nội thất,...
        """)

    tab_predict, tab_about = st.tabs(["🔮 Dự báo xu hướng", "📊 Thông tin mô hình"])

    # ----- TAB 1: Dự báo -----
    with tab_predict:
        st.markdown("Nhập thông tin bất động sản bạn đang quan tâm:")

        with st.form("prediction_form"):
            col1, col2 = st.columns(2)

            with col1:
                quan = st.selectbox("📍 Quận / Huyện", all_locations)
                loai = st.selectbox("🏠 Loại bất động sản", all_types)

            with col2:
                dt = st.number_input("📐 Diện tích (m²)", min_value=10.0, value=50.0)
                mt = st.selectbox("🛣️ Có mặt tiền đường không?", ["Không", "Có"])
                ls = st.number_input(
                    "🏦 Lãi suất điều hành hiện tại (%)",
                    value=4.5,
                    step=0.1,
                    help="Mặc định là lãi suất qua đêm của Ngân hàng Nhà nước."
                )

            st.caption("💡 Nếu bạn không rõ về lãi suất, hãy giữ nguyên giá trị gợi ý.")
            submitted = st.form_submit_button("🔮 Thực hiện dự báo")

        if submitted:
            mat_tien_val = 1 if mt == "Có" else 0
            result = bot.predict(quan, loai, dt, mat_tien_val, ls)

            if result is None:
                st.error(f"⚠️ Chưa có đủ dữ liệu lịch sử cho {loai} tại {quan} để dự báo.")
            else:
                pred, prob, vithe = result

                # Card kịch bản nhập vào
                st.markdown(
                    f"""
                    <div class="result-card">
                        <div style="font-size:0.85rem;color:#9ca3af;margin-bottom:0.3rem;">
                            Kịch bản bạn vừa nhập:
                        </div>
                        <div class="scenario-text">
                            <b>{quan}</b> · {loai.lower()} · {dt:.0f} m² · 
                            {"có mặt tiền đường" if mat_tien_val == 1 else "không mặt tiền đường"} ·
                            lãi suất {ls:.2f}%
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                c1, c2, c3 = st.columns(3)

                # Xu hướng
                if pred == 1:
                    c1.subheader("Xu hướng")
                    c1.markdown("### TĂNG GIÁ 📈")
                    c1.markdown('<span class="badge-up">Tích cực</span>', unsafe_allow_html=True)
                else:
                    c1.subheader("Xu hướng")
                    c1.markdown("### ĐI NGANG / GIẢM 📉")
                    c1.markdown('<span class="badge-down">Tiêu cực / chững lại</span>', unsafe_allow_html=True)

                # Độ tin cậy
                c2.subheader("Độ tin cậy")
                c2.markdown(f"### {prob*100:.1f}%")
                c2.progress(min(max(prob, 0.0), 1.0))

                # Vị thế giá + expander tuỳ theo vithe
                c3.subheader("Vị thế giá so với tuần trước")
                c3.markdown(f"### {vithe:.4f}")

                with c3.expander("👉 Xem giải thích ý nghĩa"):
                    if vithe > 0.05:
                        st.markdown("""
**Kết luận nhanh:** Giá trung bình hiện tại đang **cao hơn** so với khoảng 1 tuần trước.

- Vị thế giá dương ( > 0 ) → khu vực đang ở **vùng giá cao hơn tuần trước**.  
- Có thể hiểu là thị trường đã có xu hướng **tăng nhẹ trong tuần vừa rồi**.
""")
                    elif vithe < -0.05:
                        st.markdown("""
**Kết luận nhanh:** Giá trung bình hiện tại đang **thấp hơn** so với khoảng 1 tuần trước.

- Vị thế giá âm ( < 0 ) → khu vực đang ở **vùng giá thấp hơn tuần trước**.  
- Có thể liên quan tới áp lực giảm giá hoặc nhu cầu yếu hơn.
""")
                    else:
                        st.markdown("""
**Kết luận nhanh:** Giá hiện tại **gần như không khác nhiều** so với khoảng 1 tuần trước.

- Vị thế giá xấp xỉ 0 → thị trường **đi ngang**, chưa có xu hướng tăng/giảm rõ ràng.
""")

                st.divider()
                # Expander diễn giải kết quả dự báo – tuỳ theo pred + prob
                with st.expander("📌 Xem diễn giải kết quả dự báo"):
                    if pred == 1:
                        trend_text = (
                            "Mô hình dự báo khu vực này có khả năng **TĂNG GIÁ** "
                            "trong khoảng 7 ngày tới."
                        )
                        reason_text = (
                            "Có thể do giá gần đây đang có xu hướng đi lên, "
                            "khối lượng tin đăng hoặc các chỉ báo khác ở mức tích cực."
                        )
                    else:
                        trend_text = (
                            "Mô hình dự báo giá có xu hướng **ĐI NGANG hoặc GIẢM** "
                            "trong khoảng 7 ngày tới."
                        )
                        reason_text = (
                            "Có thể do giá đã ở vùng cao, khối lượng tin đăng chững lại "
                            "hoặc điều kiện vĩ mô kém thuận lợi."
                        )

                    if prob >= 0.7:
                        conf_text = (
                            "Độ tin cậy đang ở mức **cao** (≥ 70%) – mô hình khá tự tin với dự báo này."
                        )
                    elif prob >= 0.4:
                        conf_text = (
                            "Độ tin cậy ở mức **trung bình** (40–70%) – nên xem đây là một tín hiệu "
                            "tham khảo và kết hợp thêm các nguồn thông tin khác."
                        )
                    else:
                        conf_text = (
                            "Độ tin cậy ở mức **thấp** (< 40%) – kết quả chỉ mang tính gợi ý, "
                            "không nên dựa hoàn toàn vào dự báo của mô hình."
                        )

                    st.markdown(f"""
**Phạm vi dự báo**  
Xu hướng cho **7 ngày tiếp theo** tính từ thời điểm dữ liệu mới nhất.

**Kết quả xu hướng**  
{trend_text}  
{reason_text}

**Độ tin cậy**  
{conf_text}

**Lưu ý**  
- Đây là công cụ hỗ trợ phân tích, *không phải* khuyến nghị đầu tư chính thức.  
- Cần xem thêm pháp lý, quy hoạch, hiện trạng thực tế bất động sản trước khi ra quyết định.
""")


                st.markdown(
                    '<div class="small-footer">'
                    'Bất Động Sản AI · Demo đồ án Data Science'
                    '</div>',
                    unsafe_allow_html=True
                )

    # ----- TAB 2: Thông tin mô hình -----
    with tab_about:
        # 1. Thời gian & phạm vi dữ liệu
        st.subheader("1. Thời gian & phạm vi dữ liệu")
        st.markdown(f"""
        - Dữ liệu được thu thập và xử lý đến **{DATA_LAST_UPDATED}**.  
        - Đối tượng: các tin đăng mua bán nhà đất tại **TP.HCM** trên một số website BĐS.  
        - Mỗi bản ghi đại diện cho **một ngày** của một cặp *(Quận/Huyện, loại bất động sản)*,  
          đã được tổng hợp (giá trung bình, diện tích trung bình, số lượng tin...).  
        """)

        # 2. Kiến trúc / pipeline mô hình
        st.subheader("2. Kiến trúc / quy trình mô hình")
        st.markdown("""
        Quy trình xây dựng mô hình của đề tài:

        1. **Thu thập dữ liệu**  
           - Crawler tin đăng BĐS TP.HCM (giá, diện tích, loại BĐS, mặt tiền, quận/huyện, thời gian đăng...).  
           - Kết hợp với **lãi suất điều hành** của Ngân hàng Nhà nước theo thời gian.

        2. **Tiền xử lý & làm sạch**  
           - Loại bỏ tin thiếu thông tin quan trọng, giá quá bất thường.  
           - Chuẩn hoá tên quận/huyện, loại bất động sản.  
           - Tổng hợp dữ liệu theo ngày & khu vực (giá trung bình, số lượng tin, diện tích trung bình...).

        3. **Tạo đặc trưng (feature engineering)**  
           - Tính các chỉ số xu hướng trong **1 tuần vừa qua** (volume, diện tích, mặt tiền...).  
           - Tính giá trung bình **1 tháng trước** để so sánh với hiện tại.  
           - Tạo các đặc trưng tương tác giữa **lãi suất** và **giá/diện tích**.

        4. **Gán nhãn xu hướng**  
           - Nếu giá trung bình giai đoạn sau cao hơn giai đoạn trước → nhãn **Tăng giá (1)**.  
           - Ngược lại → **Đi ngang / Giảm (0)**.

        5. **Huấn luyện mô hình**  
           - Sử dụng thuật toán **LightGBM** cho bài toán phân loại nhị phân.  
           - Chia tập train/test, đánh giá bằng Accuracy, F1-score, ROC-AUC.

        6. **Triển khai mô hình**  
           - Lưu model đã huấn luyện và các thông tin tham chiếu ra file `.pkl`.  
           - Xây dựng web demo bằng **Streamlit** để người dùng nhập kịch bản & xem kết quả dự báo.
        """)

        # 3. Điểm mạnh của mô hình
        st.subheader("3. Điểm mạnh của mô hình")
        st.markdown("""
        - **Kết hợp dữ liệu vĩ mô & vi mô**: vừa xem được hành vi giá/tin đăng tại từng khu vực,
          vừa phản ánh tác động của lãi suất thị trường.  
        - **Tập trung vào xu hướng ngắn hạn (1 tuần)**: phù hợp với nhu cầu theo dõi diễn biến thị trường.  
        - **Triển khai nhanh**: mô hình nhẹ, dự báo tức thì qua web Streamlit.  
        - **Dễ mở rộng**: có thể bổ sung thêm đặc trưng khác (GDP, CPI, quy hoạch...) mà không cần thay đổi kiến trúc tổng thể.
        """)

        # 4. Ý nghĩa chỉ số + hạn chế
        st.subheader("4. Ý nghĩa chỉ số & hạn chế")
        st.markdown("""
        **Ý nghĩa các chỉ số hiển thị trên web:**

        - **Xu hướng:** kết quả chính của mô hình – Trong 7 ngày tới, giá có xu hướng **tăng** hay **đi ngang/giảm**.  
        - **Độ tin cậy:** xác suất mô hình tin rằng dự báo đó là đúng (0–100%).  
        - **Vị thế giá so với tuần trước:**  
          - Dương (> 0): giá trung bình hiện tại cao hơn so với khoảng 1 tuần trước.  
          - Âm (< 0): giá trung bình hiện tại thấp hơn so với khoảng 1 tuần trước.

        **Hạn chế của mô hình:**

        - Chỉ dựa trên **dữ liệu lịch sử** nên không thể dự đoán chính xác 100% tương lai.  
        - Chưa xét đến các yếu tố quan trọng khác:
          - Tình trạng pháp lý của bất động sản.  
          - Quy hoạch chi tiết, hạ tầng xung quanh.  
          - Nội thất, vị trí trong hẻm, phong thủy, v.v.  
        - Dữ liệu tập trung vào **TP.HCM**, nên không thể suy rộng sang các tỉnh/thành khác.  

        👉 Vì vậy, kết quả mô hình chỉ nên dùng như **một nguồn thông tin tham khảo** khi phân tích thị trường,
        không phải khuyến nghị đầu tư chính thức.
        """)

except Exception as e:
    st.error("⚠️ Lỗi: Không tìm thấy hoặc không load được file model. Hãy đảm bảo đã train và lưu model (.pkl).")
    st.write(e)
