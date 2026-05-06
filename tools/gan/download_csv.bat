@echo off
setlocal

set REPO_SSH=ssh://git@lisdi-git.lanl.gov:10022/dsi/gan_data_sources.git
set BRANCH=main
set CSV_PATH=gan_data_path.csv

git archive --remote=%REPO_SSH% %BRANCH% %CSV_PATH% | tar -x -C .

if %errorlevel% neq 0 (
    echo Error occurred during archive or extraction
    exit /b %errorlevel%
)

echo File extracted successfully

endlocal