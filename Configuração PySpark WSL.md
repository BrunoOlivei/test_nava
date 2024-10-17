# Guide to install PySpark on Windows Subsystem for Linux (WSL):

1. Install WSL:
   - Open PowerShell as Administrator and run:
     ```
     wsl --install
     ```
   - Restart your computer when prompted.

2. Set up Ubuntu on WSL:
   - After restart, a Ubuntu terminal will open automatically.
   - Create a username and password when prompted.

3. Update and upgrade Ubuntu:
   ```
   sudo apt update && sudo apt upgrade -y
   ```

4. Install Java:
   ```
   sudo apt install default-jdk -y
   ```
   Verify installation:
   ```
   java -version
   ```

5. Install Python and pip:
   ```
   sudo apt install python3 python3-pip -y
   ```
   Verify installation:
   ```
   python3 --version
   pip3 --version
   ```

6. Install PySpark:
   ```
   pip3 install pyspark
   ```

7. Set up environment variables:
   - Open the bash profile:
     ```
     nano ~/.bashrc
     ```
   - Add the following lines at the end of the file:
     ```
     export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
     export SPARK_HOME=/home/your_username/.local/lib/python3.8/site-packages/pyspark
     export PATH=$PATH:$SPARK_HOME/bin
     export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
     ```
   - Replace `your_username` with your actual username.
   - Save and exit (Ctrl+X, then Y, then Enter).
   - Apply changes:
     ```
     source ~/.bashrc
     ```

8. Verify PySpark installation:
   - Start Python:
     ```
     python3
     ```
   - In the Python interpreter, try:
     ```python
     from pyspark.sql import SparkSession
     spark = SparkSession.builder.getOrCreate()
     spark
     ```
   - If you see SparkSession information without errors, PySpark is installed correctly.

9. (Optional) Install Jupyter Notebook for interactive PySpark usage:
   ```
   pip3 install jupyter
   ```
   To use PySpark with Jupyter, start it with:
   ```
   PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook pyspark
   ```
