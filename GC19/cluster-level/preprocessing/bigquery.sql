SELECT MIN(time) as arrival_time, collection_id, SUM(reqcpu*interval) as reqcpusecs, SUM(reqmem*interval) as reqmemsecs, MIN(priority), SUM(interval) as tot_interval, SUM(avgcpu*interval) as avgcpusecs, SUM(avgmem*interval) as avgmemsecs, SUM(maxcpu*interval) as maxcpusecs, SUM(maxmem*interval) as maxmemsecs
FROM (
(SELECT time, collection_id, collection_type, resource_request.cpus AS reqcpu, resource_request.memory as reqmem, priority, type, instance_index
FROM `google.com:google-cluster-data`.clusterdata_2019_b.instance_events
WHERE type=0) 
INNER JOIN
(SELECT end_time-start_time as interval, collection_id, instance_index, average_usage.cpus as avgcpu, average_usage.memory as avgmem, maximum_usage.cpus as maxcpu, maximum_usage.memory as maxmem
FROM `google.com:google-cluster-data`.clusterdata_2019_b.instance_usage
)
USING (collection_id, instance_index)
GROUP BY (collection_id))

