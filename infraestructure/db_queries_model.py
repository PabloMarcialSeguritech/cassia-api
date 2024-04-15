class DBQueries:

    def __init__(self):
        self.stored_name_get_connectivity_data_m = 'sp_connectivityM'
        self.stored_name_get_connectivity_data = 'sp_connectivity'
        self.query_statement_get_alineacion_group_id = "select group_id from metric_group mg where nickname = 'Alineación'"
        self.stored_name_get_aligment_report_data_m = 'sp_alignmentReportM'
        self.stored_name_get_aligment_report_data = 'sp_alignmentReport'
        self.stored_name_get_host_health_detail_data = 'sp_hostHealt'
        self.stored_name_get_host_view_data = 'sp_hostView'
        self.stored_name_get_acknowledges = "sp_acknowledgeList1"
        self.stored_name_get_dependents_diagnostic_problems = "sp_diagnostic_problemsD"
        self.query_get_open_diagnosta_problems = "select * from cassia_diagnostic_problems_2 cdp where cdp.closed_at is NULL"
        self.query_get_total_slack_notifications_count = "select count(notification_id) as notificaciones from cassia_slack_notifications"
        self.query_statement_get_metrics_template = None
        self.query_statement_get_host_correlation = None
        self.stored_name_problems_severity = 'sp_problembySev'
        self.query_statement_get_arch_traffic_events_date_close_null = "select * from cassia_arch_traffic_events WHERE closed_at IS NULL"
        self.stored_name_catalog_city = 'sp_catCity'
        self.query_statement_get_arch_traffic_events_date_close_null_municipality_template = None
        self.stored_name_get_host_available_ping_loss_data = 'sp_hostAvailPingLoss'
        self.query_statement_get_config_data_by_name = None
        self.stored_name_get_view_problem_data = 'sp_viewProblem'
        self.stored_name_get_diagnostic_problems = 'sp_diagnostic_problems1'
        self.query_statement_get_data_problems_by_list_ids = None
        self.stored_name_get_diagnostic_problems_d = 'sp_diagnostic_problemsD'
        self.query_statement_get_total_synchronized_data = "select * from cassia_diagnostic_problems_2 cdp where cdp.closed_at is NULL"
        self.stored_name_get_metric_view_h_data = 'sp_MetricViewH'
        self.stored_name_get_switch_through_put_data = 'sp_switchThroughtput'
        self.stored_name_get_alerts = "sp_viewProblem"
        self.stored_name_city_catalog = "sp_catCity"
        self.query_statement_get_cassia_events_acknowledges = """select cea.eventid , cea.message as message from (
        select eventid,MAX(cea.acknowledgeid) acknowledgeid
        from cassia_event_acknowledges cea group by eventid ) ceaa
        left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid"""
        self.stored_name_diagnostic_problems_origen_1 = 'sp_diagnostic_problems1'
        self.stored_name_get_connectivity_data_by_device = "sp_connectivityHost"
        self.query_statement_get_device_alineacion = """select DISTINCT device_id from metrics_template mt 
        inner join metric_group mg on mg.group_id =mt.group_id 
        where nickname ='Alineación'"""
        self.stored_name_get_aligment_report_data_by_device = "sp_alignmentReport_Host"

    def builder_query_statement_get_metrics_template(self, tech_id, alineacion_id):
        self.query_statement_get_metrics_template = f"""select * from metrics_template mt where device_id ='{tech_id}' and group_id ='{alineacion_id}'"""
        return self.query_statement_get_metrics_template

    def builder_query_statement_get_cassia_event(self, eventid):
        self.query_statement_get_cassia_event = f"""select cassia_arch_traffic_events_id,created_at  from cassia_arch_traffic_events p where cassia_arch_traffic_events_id ='{eventid}'"""
        return self.query_statement_get_cassia_event

    def builder_query_statement_get_cassia_event_tickets(self, eventid):
        self.query_statement_get_cassia_event_tickets = f"select * from cassia_tickets where event_id ='{eventid}'"
        return self.query_statement_get_cassia_event_tickets

    def builder_query_statement_get_zabbix_event(self, eventid):
        self.query_statement_get_zabbix_event = f"select eventid,clock  from events p where eventid ='{eventid}'"
        return self.query_statement_get_zabbix_event

    def builder_query_statement_get_last_zabbix_event_acknowledge(self, eventid):
        self.query_statement_get_last_zabbix_event_acknowledge = f"select acknowledgeid from acknowledges where eventid={eventid} order by acknowledgeid desc limit 1"
        return self.query_statement_get_last_zabbix_event_acknowledge

    def builder_query_statement_get_hots_zabbix_alerts(self, hostid):
        self.query_statement_get_hots_zabbix_alerts = f"""
SELECT from_unixtime(p.clock,'%d/%m/%Y %H:%i:%s' ) as Time,
	p.severity,h.hostid,h.name Host,hi.location_lat as latitude,hi.location_lon as longitude,
	it.ip,p.name Problem, IF(ISNULL(p.r_eventid),'PROBLEM','RESOLVED') Estatus, p.eventid,p.r_eventid,
	IF(p.r_clock=0,'',From_unixtime(p.r_clock,'%d/%m/%Y %H:%i:%s' ) )'TimeRecovery',
	p.acknowledged Ack,IFNULL(a.Message,'''') AS Ack_message FROM hosts h
	INNER JOIN host_inventory hi ON (h.hostid=hi.hostid)
	INNER JOIN interface it ON (h.hostid=it.hostid)
	INNER JOIN items i ON (h.hostid=i.hostid)
	INNER JOIN functions f ON (i.itemid=f.itemid)
	INNER JOIN triggers t ON (f.triggerid=t.triggerid)
	INNER JOIN problem p ON (t.triggerid = p.objectid)
	LEFT JOIN acknowledges a ON (p.eventid=a.eventid)
	WHERE  h.hostid={hostid}
	ORDER BY p.clock  desc
    limit 20
"""
        return self.query_statement_get_hots_zabbix_alerts

    def builder_query_statement_get_hots_cassia_alerts(self, hostid):
        self.query_statement_get_hots_cassia_alerts = f"""
select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message  from cassia_arch_traffic_events_2 cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_2 cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id
where cate.hostid ={hostid} order by cate.created_at desc limit 20
"""
        return self.query_statement_get_hots_cassia_alerts

    def builder_query_statement_get_local_events_diagnosta(self, hostid):
        self.query_statement_get_local_events_diagnosta = f"select local_eventid from cassia_diagnostic_problems_2 where hostid={hostid}"
        return self.query_statement_get_local_events_diagnosta

    def builder_query_statement_get_config_value_by_name(self, name):
        self.query_statement_get_config_value_by_name = f"""SELECT cassia_config.config_id, cassia_config.name, cassia_config.data_type, cassia_config.value
FROM cassia_config where cassia_config.name='{name}'"""
        return self.query_statement_get_config_value_by_name

    def builder_query_statement_get_user_slack_notification(self, userid):
        self.query_statement_get_user_slack_notification = f"""SELECT * FROM cassia_slack_user_notifications WHERE user_id={userid}"""
        return self.query_statement_get_user_slack_notification

    def builder_query_statement_get_user_slack_notification_count(self, last_date):
        self.query_statement_get_user_slack_notification_count = f"""select count(notification_id) as notificaciones from cassia_slack_notifications csn where message_date>'{last_date}'"""
        return self.query_statement_get_user_slack_notification_count

    def builder_query_statement_get_slack_notifications(self, skip, limit):
        self.query_statement_get_slack_notifications = f"""select * from cassia_slack_notifications csn
            order by message_date desc
            limit {limit} offset {skip}"""
        return self.query_statement_get_slack_notifications

    def builder_query_statement_update_user_slack_notification(self, user_id, date):
        self.query_statement_update_user_slack_notification = f"""update cassia_slack_user_notifications set last_date='{date}' where user_id={user_id}"""
        return self.query_statement_update_user_slack_notification

    def builder_query_statement_get_metrics_template(self, tech_id, alineacion_id):
        self.query_statement_get_metrics_template = f"""select * from metrics_template mt where device_id ='{tech_id}' and group_id ='{alineacion_id}'"""
        return self.query_statement_get_metrics_template

    def builder_query_statement_get_host_correlation(self, hostids):
        self.query_statement_get_host_correlation = f"""
                SELECT hc.correlarionid,
                hc.hostidP,
                hc.hostidC,
                (SELECT location_lat from host_inventory where hostid=hc.hostidP) as init_lat,
                (SELECT location_lon from host_inventory where hostid=hc.hostidP) as init_lon,
                (SELECT location_lat from host_inventory where hostid=hc.hostidC) as end_lat,
                (SELECT location_lon from host_inventory where hostid=hc.hostidC) as end_lon
                from host_correlation hc
                where (SELECT location_lat from host_inventory where hostid=hc.hostidP) IS NOT NULL
                and
                (
                hc.hostidP in {hostids}
                and hc.hostidC in {hostids})
                """
        return self.query_statement_get_host_correlation

    def builder_query_statement_get_arch_traffic_events_date_close_null_municipality(self, municipality):
        self.query_statement_get_arch_traffic_events_date_close_null_municipality_template = f"""select * from cassia_arch_traffic_events WHERE closed_at IS NULL and municipality ='{municipality}'"""
        return self.query_statement_get_arch_traffic_events_date_close_null_municipality_template

    def builder_query_statement_get_config_data_by_name(self, name):
        self.query_statement_get_config_data_by_name = f"""select * from cassia_config where name='{name}'"""
        return self.query_statement_get_config_data_by_name

    def builder_query_statement_get_data_problems(self, list_hosts_downs_origen_ids):
        self.query_statement_get_data_problems_by_list_ids = f"""
            select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_arch_traffic_events_2 cate
            left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
            from cassia_event_acknowledges cea group by eventid ) as ceaa
            on  cate.cassia_arch_traffic_events_id=ceaa.eventid
            left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
            left join cassia_diagnostic_problems_2 cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id
            where cate.closed_at is NULL and cate.hostid in {list_hosts_downs_origen_ids}"""
        return self.query_statement_get_data_problems_by_list_ids

    def builder_query_statement_get_global_cassia_events_by_tech(self, tech_id):
        self.query_statement_get_global_cassia_events_by_tech = f"""
        SELECT * FROM cassia_arch_traffic_events where closed_at is NULL and tech_id={tech_id}"""
        return self.query_statement_get_global_cassia_events_by_tech

    def builder_query_statement_get_cassia_events_by_tech_and_municipality(self, municipality, tech_id):
        self.query_statement_get_cassia_events_by_tech_and_municipality = f"""
        SELECT * FROM cassia_arch_traffic_events where closed_at is NULL and tech_id={tech_id} and municipality='{municipality}'"""
        return self.query_statement_get_cassia_events_by_tech_and_municipality

    def builder_query_statement_get_cassia_events_with_hosts_filter(self, hostids):
        self.query_statement_get_cassia_events_with_hosts_filter = f"""select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_arch_traffic_events_2 cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_2 cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id 
where cate.closed_at is NULL and cate.hostid in ({hostids})"""
        return self.query_statement_get_cassia_events_with_hosts_filter

    def builder_query_statement_get_pertenencia_host_metric(self, hostid, metricid):
        self.query_statement_get_pertenencia_host_metric = f"""select * from hosts h
        inner join host_inventory hi 
        on h.hostid =hi.hostid 
        where hi.hostid ={hostid}
        and hi.device_id ={metricid}"""
        return self.query_statement_get_pertenencia_host_metric
