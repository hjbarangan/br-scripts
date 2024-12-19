python3 -m venv venv
source venv/bin/activate  # On Linux
source venv\Scripts\activate     # On Windows

pip install pyodbc python-dotenv

pip freeze > requirements.txt


export http_proxy=http://192.168.36.35:3128 # Proxy