DuckDB Python Docker Image - Build Instructions
Step 1: Prepare Project Folder
Create a project folder (e.g., chicago-benchmark) and a data subfolder. Place your DuckDB file inside data/.

chicago-benchmark/
├─ data/
│  └─ chicago_311.duckdb
├─ Dockerfile.duckdb-python
└─ README_BUILD.md
Step 2: Create Dockerfile.duckdb-python
Create a file named Dockerfile.duckdb-python in your project root:

dockerfile
# Use lightweight Python base image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /data

# Install DuckDB and Pandas
RUN pip install --no-cache-dir duckdb pandas

# Mount /data from host for persistence
VOLUME ["/data"]

# Default command: start Python REPL
CMD ["python3"]
Step 3: Build the Docker Image
Run this command from your project root:

bash
docker build -t chicago-benchmark-duckdb-python:latest -f Dockerfile.duckdb-python .

Command breakdown:

-t → gives a name/tag to the image
-f → specifies the Dockerfile to use
. → build context (current folder)
Verification
Check that your image was built successfully:

bash
docker images
You should see chicago-benchmark-duckdb-python in the list.

✅ Your DuckDB Python Docker image is now ready to use!

Troubleshooting Build Issues
Docker not found:

Ensure Docker Desktop is installed and running
Verify with: docker --version
Build fails:

Check that you're in the correct directory (where Dockerfile.duckdb-python exists)
Try building with --no-cache flag: docker build --no-cache -t chicago-benchmark-duckdb-python:latest -f Dockerfile.duckdb-python .
Permission errors:

On Windows: Run PowerShell/CMD as Administrator
On Mac/Linux: Ensure your user is in the docker group
