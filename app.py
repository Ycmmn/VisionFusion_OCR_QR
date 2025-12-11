# -*- coding: utf-8 -*-
"""
 smart exhibition pipeline — final unified edition + google sheets
complete merge of two apps: «ultimate smart exhibition pipeline» + «smart data pipeline»
- awesome ui version 1 + logic, logging & quota management version 2
- excel mode and ocr/qr mode with automatic detection
- smart metadata injection (exhibition + source + smart position)
- fast mode, debug mode, rate limiting, daily quota
- batch processing: images(5), pdfs(4), excel(1)
- quality control tracking: user name, role, date, time
- google sheets integration: automatic data storage in google drive

run:
    streamlit run smart_exhibition_pipeline_final.py
"""



#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ page configuration ^^^^^^^^^^^^^^^^^^^^
st.set_page_config(
    page_title="Smart Exhibition Pipeline",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)


#^^^^^^^^^^^^^^^^ permanent google sheets link (always visible)^^^^^^^^^^^^^^
FIXED_SHEET_URL = "https://docs.google.com/spreadsheets/d/1OeQbiqvo6v58rcxaoSUidOk0IxSGmL8YCpLnyh27yuE/edit"

st.markdown(f"""
<div style="
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1.2rem; border-radius: 15px; text-align: center; color: white;
    box-shadow: 0 6px 20px rgba(102,126,234,0.4); margin-bottom: 1.5rem;">
    <h3 style="margin: 0;">📊 central data sheet</h3>
    <a href="{FIXED_SHEET_URL}" target="_blank"
       style="color: white; background: rgba(255,255,255,0.2);
              padding: 0.6rem 1.2rem; border-radius: 10px;
              text-decoration: none; display: inline-block; margin-top: 0.5rem;">
        🔗 open in google sheets
    </a>
    <p style="margin-top: 0.5rem; font-size: 0.85rem; opacity: 0.9;">
        all processed data are automatically saved here
    </p>
</div>
""", unsafe_allow_html=True)