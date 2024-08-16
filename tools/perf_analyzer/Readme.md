## PerfAnalyzer

A tool to analze software performance along with code history. This is built on top of DSI SQLite plugin.

Run `fly_server.py` file. Then the dashboard can be accessed through `http://127.0.0.1:8050/`.

##### TODO: add a requirement file

Update the `runner_script.sh` to compile, copy input file, and run the program.

Update the `parse_clover_output.py` file and update `parse_clover_output_file` function to parse specific output file. Return a dictionary containing the contents of the parsed output file.