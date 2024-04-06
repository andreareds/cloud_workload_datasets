from google.cloud import bigquery
import pandas as pd
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'steam-shape-278619-3b26e69d872b.json'

clusters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
for c in clusters:
    all_machines_query = """SELECT DISTINCT machine_id
                            FROM `google.com:google-cluster-data`.clusterdata_2019_"""+c+""".machine_events
                            ORDER BY machine_id"""
    query_job = client.query(all_machines_query)

    machines = query_job.result().to_dataframe()  # Waits for job to complete.
    machines.to_csv("/Users/arossi/Desktop/BigQuery/machines_"+c+".csv")

    for machine in machines.machine_id:
        query_machine = """SELECT CAST(start_time/300000000 as int) as slot, SUM(intervall/1000000) as intervall, COUNT(instance_index) as counts, SUM(pavgcpu * intervall) as avgcpu, SUM(pavgmem * intervall) as avgmem, SUM(pmaxcpu * intervall) as maxcpu, SUM(pmaxmem * intervall) as maxmem
                            FROM
                            (SELECT machine_id, start_time as start_time, end_time-start_time as intervall, instance_index as instance_index, average_usage.cpus as pavgcpu, average_usage.memory as pavgmem, maximum_usage.cpus as pmaxcpu, maximum_usage.memory as pmaxmem
                            FROM `google.com:google-cluster-data`.clusterdata_2019_"""+c+""".instance_usage
                            WHERE machine_id = """+ str(machine)+""")
                            GROUP BY slot
                            ORDER BY slot
                            """
        query_job = client.query(query_machine)

        machine_df = query_job.result().to_dataframe()  # Waits for job to complete.
        print(c, machine, '\n', machine_df)
        machine_df.to_csv("/Users/arossi/Desktop/BigQuery/"+c+"/machine_"+str(machine).zfill(12)+".csv")
