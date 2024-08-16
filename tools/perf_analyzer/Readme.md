## PerfAnalyzer

A tool to analze software performance along with code history. This is built on top of DSI SQLite plugin.

Run `fly_server.py` file. Then the dashboard can be accessed through `http://127.0.0.1:8050/`.

##### TODO: add a requirement file

Update the `runner_script.sh` to compile, copy input file, and run the program.

Update the `parse_clover_output.py` file and update `parse_clover_output_file` function to parse specific output file. Return a dictionary containing the contents of the parsed output file.

#### The features available in the dashboard

PerfAnalyzer is a dashboard based visualizer to analyze performance using git commit history and different performance metric. This has the following features

- Git History Graph
  - Ordered by commit dates
  - Filter Git Branch
  - Select a subset of git commits 
  - Show Commit details like message, committer name, date, and hash.
- Performance metric line chart
  - Filter by different metric
  - Show details on hover
 - Commit table
   - Search and filter by date, hash, and message.
   - Execute the `runner_script` on selected commit.
   - Show difference between two commits (using git diff)
 - Variable Search
   - Use any regex or string to search
   - Show table of found variable and file
   - Show file content