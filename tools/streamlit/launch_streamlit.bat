@echo off

set PORT=8501
SET APP=%1

REM Run the Streamlit app
streamlit run %APP%  --server.port=%PORT% --server.headless=true --browser.gatherUsageStats=false %*

pause