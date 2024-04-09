class DBQueries:

    def __init__(self):
        self.stored_name_get_connectivity_data_m = 'sp_connectivityM'
        self.stored_name_get_connectivity_data = 'sp_connectivity'
        self.query_statement_get_alineacion_group_id = "select group_id from metric_group mg where nickname = 'Alineación'"
        self.stored_name_get_aligment_report_data_m = 'sp_alignmentReportM'
        self.stored_name_get_acknowledges = "sp_acknowledgeList1"
        self.stored_name_get_dependents_diagnostic_problems = "sp_diagnostic_problemsD"
        self.query_get_open_diagnosta_problems = "select * from cassia_diagnostic_problems_2 cdp where cdp.closed_at is NULL"
        self.query_get_total_slack_notifications_count = "select count(notification_id) as notificaciones from cassia_slack_notifications"

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
