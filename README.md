# webscrape
webscrape task for deep learning model


# Install Airflow (this takes a few minutes)
- in the env or project venv 
pip install "apache-airflow[celery]==3.0.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt"

Step 2:  Add to Shell Config
bash# For Mac/Linux with zsh
echo "export AIRFLOW_HOME=~/path/to/your/project/webscrape/airflow" >> ~/.zshrc
source ~/.zshrc

# For Mac/Linux with bash
echo "export AIRFLOW_HOME=~/path/to/your/project/webscrape/airflow" >> ~/.bashrc
source ~/.bashrc

# For Windows, add to System Environment Variables via Settings

# Verify AIRFLOW_HOME is set
echo $AIRFLOW_HOME  # Should show your project path + /airflow

Step 3: # Initialize the database
python -m airflow db migrate
```

a new `airflow/` folder will appear in your project:
```

airflow/
├── airflow.cfg
├── airflow.db
├── dags/
├── logs/
└── webserver_config.py
