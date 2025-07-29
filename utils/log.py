import logging
import os
import sys
import datetime
import paramiko
from scp import SCPClient
def create_log_dir():
    if not os.environ.get('log_dir'):
        script_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
        script_dir = os.path.join(script_dir, 'logs')
        current_time = datetime.datetime.now().strftime("%d-%m-%y-%I-%p-%M-%S")
        folder_name = f"folder_{current_time}"
        log_dir = os.path.join(script_dir, folder_name)
        os.makedirs(log_dir, exist_ok=True)
        os.environ.setdefault('log_dir', log_dir)

def set_logger(log_file_name, dir=None):
    if not os.environ.get('log_dir'):
        create_log_dir()
    log_dir = os.environ.get('log_dir')
    if dir is not None:
        log_dir = os.path.join(log_dir, dir)
        os.makedirs(log_dir, exist_ok=True)
    log_filename = log_file_name + '.log'
    log_filepath = os.path.join(log_dir, log_filename)
    if os.path.exists(log_filepath):
        os.remove(log_filepath)
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_filepath, mode='a')
    file_handler.setLevel(logging.INFO)  # Level for this handler
    format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    formatter = logging.Formatter(format)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # Level for this handler
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

def create_report(hosts):
    def status_color(db):
        return 'green' if db.is_healthy else 'red'
    def status_health(db):
        return 'healthy' if db.is_healthy else 'unhealthy'

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Database Health Report</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                padding: 20px;
            }}
            h2 {{
                color: #333;
            }}
            table {{
                border-collapse: collapse;
                width: 50%;
                margin-bottom: 30px;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 8px 12px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <h1>Database Health Report</h1>
        <p>Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    """

    for host in hosts:
        dbs = host.dbs
        html += f"<h2>{host}</h2>\n"
        html += "<table>\n<tr><th>Database</th><th>Health Status</th></tr>\n"
        for db in dbs:
            color = status_color(db)
            status = status_health(db)
            html += f"<tr><td>{db}</td><td style='color:{color}; font-weight:bold'>{status}</td></tr>\n"
        html += "</table>\n"

    html += "</body>\n</html>"
    log_dir = os.environ.get('log_dir')
    reports_file = os.path.join(log_dir, 'db_health_report.html')
    # Save to file
    with open(reports_file, "w") as f:
        f.write(html)

    print("HTML report saved as db_health_report.html")

def scp_to_remote(local_path, remote_host, remote_user, remote_path, password=None, port=22, key_file=None):
    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if key_file:
            ssh.connect(remote_host, port=port, username=remote_user, key_filename=key_file)
        else:
            ssh.connect(remote_host, port=port, username=remote_user, password=password)
        stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {remote_path}")
        stdout.channel.recv_exit_status()  # Wait for command to finish
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(local_path, remote_path, recursive=True)

        print(f"✅ File {local_path} copied to {remote_user}@{remote_host}:{remote_path}")

    except Exception as e:
        print(f"❌ Failed to SCP file: {e}")
    finally:
        ssh.close()


