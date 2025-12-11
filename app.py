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