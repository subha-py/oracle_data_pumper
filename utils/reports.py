import os
import datetime
import urllib.parse
from utils.cohesity import get_cluster_name
def create_report(hosts, cluster_ip):

    def status_health(db):
        return 'healthy ✅' if db.is_healthy else 'unhealthy ❌'

    def host_color(host):
        return 'green' if host.is_healthy else 'red'

    def host_status_emoji(host):
        return '🟢' if host.is_healthy else '🔴'

    cluster_name = get_cluster_name(cluster_ip)
    log_dir = os.environ.get('log_dir', '.')
    last_directory = os.path.basename(os.path.normpath(log_dir))
    base_url = f"https://sv4-pluto.eng.cohesity.com/bugs/sbera_backups/oracle_pumper_dumps/{cluster_name}/{last_directory}"

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
            h1 {{
                color: #2c3e50;
            }}
            h2 {{
                margin-top: 40px;
            }}
            h3 {{
                margin-top: 20px;
                color: #555;
            }}
            table {{
                border-collapse: collapse;
                width: 60%;
                margin-bottom: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 10px;
                text-align: left;
                cursor: pointer;
            }}
            th {{
                background-color: #f9f9f9;
            }}
            tr.healthy-row {{
                background-color: #e6ffe6;
            }}
            tr.unhealthy-row {{
                background-color: #ffe6e6;
            }}
            tr:hover {{
                background-color: #f1f1f1;
            }}
            .filter-buttons {{
                margin-bottom: 10px;
            }}
            .hidden {{
                display: none;
            }}
            a {{
                text-decoration: none;
                color: inherit;
            }}
            a:hover {{
                text-decoration: underline;
            }}
        </style>
        <script>
            function toggleTable(id) {{
                document.getElementById(id).classList.toggle('hidden');
            }}

            function sortTable(tableId, colIndex) {{
                var table = document.getElementById(tableId);
                var switching = true;
                var dir = "asc";
                var switchCount = 0;

                while (switching) {{
                    switching = false;
                    var rows = table.rows;
                    for (var i = 1; i < rows.length - 1; i++) {{
                        var shouldSwitch = false;
                        var x = rows[i].getElementsByTagName("TD")[colIndex];
                        var y = rows[i + 1].getElementsByTagName("TD")[colIndex];

                        if (dir === "asc") {{
                            if (x.innerText.toLowerCase() > y.innerText.toLowerCase()) {{
                                shouldSwitch = true;
                                break;
                            }}
                        }} else if (dir === "desc") {{
                            if (x.innerText.toLowerCase() < y.innerText.toLowerCase()) {{
                                shouldSwitch = true;
                                break;
                            }}
                        }}
                    }}
                    if (shouldSwitch) {{
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        switchCount++;
                    }} else {{
                        if (switchCount === 0 && dir === "asc") {{
                            dir = "desc";
                            switching = true;
                        }}
                    }}
                }}
            }}
        </script>
    </head>
    <body>
        <h1>Database Health Report</h1>
        <p>Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    """

    table_counter = 0

    for host in hosts:
        host_str = str(host)
        host_color_code = host_color(host)
        host_emoji = host_status_emoji(host)

        # Encode filename for .log and path separately
        host_filename = urllib.parse.quote(f"{host_str}.log")
        host_path = urllib.parse.quote(host_str)
        host_log_url = f"{base_url}/hosts/{host_filename}"

        html += f"<h2 style='color:{host_color_code}'>{host_emoji} <a href='{host_log_url}' target='_blank'>{host_str} 📥</a></h2>\n"

        healthy_dbs = [db for db in host.dbs if db.is_healthy]
        unhealthy_dbs = [db for db in host.dbs if not db.is_healthy]

        if healthy_dbs:
            table_counter += 1
            table_id = f"healthy-table-{table_counter}"
            html += f"""
                <div class='filter-buttons'>
                    <button onclick="toggleTable('{table_id}')">Toggle Healthy Databases</button>
                </div>
                <h3>✅ Healthy Databases</h3>
                <table id="{table_id}">
                    <tr>
                        <th onclick="sortTable('{table_id}', 0)">Database</th>
                        <th onclick="sortTable('{table_id}', 1)">Status</th>
                    </tr>
            """
            for db in healthy_dbs:
                db_str = str(db)
                db_filename = urllib.parse.quote(f"{db_str}.log")
                db_link = f"{base_url}/dbs/{db_filename}"
                html += f"<tr class='healthy-row'><td><a href='{db_link}' target='_blank'>{db_str} 📥</a></td><td><b>{status_health(db)}</b></td></tr>\n"
            html += "</table>\n"

        if unhealthy_dbs:
            table_counter += 1
            table_id = f"unhealthy-table-{table_counter}"
            html += f"""
                <div class='filter-buttons'>
                    <button onclick="toggleTable('{table_id}')">Toggle Unhealthy Databases</button>
                </div>
                <h3>❌ Unhealthy Databases</h3>
                <table id="{table_id}">
                    <tr>
                        <th onclick="sortTable('{table_id}', 0)">Database</th>
                        <th onclick="sortTable('{table_id}', 1)">Status</th>
                    </tr>
            """
            for db in unhealthy_dbs:
                db_str = str(db)
                db_filename = urllib.parse.quote(f"{db_str}.log")
                db_link = f"{base_url}/dbs/{db_filename}"
                html += f"<tr class='unhealthy-row'><td><a href='{db_link}' target='_blank'>{db_str} 📥</a></td><td><b>{status_health(db)}</b></td></tr>\n"
            html += "</table>\n"

    html += "</body>\n</html>"

    report_path = os.path.join(log_dir, 'db_health_report.html')
    with open(report_path, "w") as f:
        f.write(html)

    print(f"HTML report saved at: {report_path}")
