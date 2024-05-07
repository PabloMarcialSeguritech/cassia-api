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
        # ACTUALIZAR NOMBRE
        self.stored_name_get_dependents_diagnostic_problems = "sp_diagnostic_problemsD"
        # ACTUALIZAR NOMBRE
        self.query_get_open_diagnosta_problems = "select * from cassia_diagnostic_problems cdp where cdp.closed_at is NULL"
        self.query_get_total_slack_notifications_count = "select count(notification_id) as notificaciones from cassia_slack_notifications"
        self.query_statement_get_metrics_template = None
        self.query_statement_get_host_correlation = None
        self.stored_name_problems_severity = 'sp_problembySev'
        # ACTUALIZAR NOMBRE
        self.query_statement_get_arch_traffic_events_date_close_null = "select * from cassia_arch_traffic_events WHERE closed_at IS NULL AND alert_type='rfid'"
        self.stored_name_catalog_city = 'sp_catCity'
        self.query_statement_get_arch_traffic_events_date_close_null_municipality_template = None
        self.stored_name_get_host_available_ping_loss_data = 'sp_hostAvailPingLoss'
        self.query_statement_get_config_data_by_name = None
        self.stored_name_get_view_problem_data = 'sp_viewProblem'
        # ACTUALIZAR NOMBRE
        self.stored_name_get_diagnostic_problems = 'sp_diagnostic_problems1'
        self.query_statement_get_data_problems_by_list_ids = None
        # ACTUALIZAR NOMBRE
        self.stored_name_get_diagnostic_problems_d = 'sp_diagnostic_problemsD'
        # ACTUALIZAR NOMBRE
        self.query_statement_get_total_synchronized_data = "select * from cassia_diagnostic_problems cdp where cdp.closed_at is NULL"
        self.stored_name_get_metric_view_h_data = 'sp_MetricViewH'
        self.stored_name_get_switch_through_put_data = 'sp_switchThroughtput'
        self.stored_name_get_alerts = "sp_viewProblem"
        self.stored_name_city_catalog = "sp_catCity"
        self.query_statement_get_cassia_events_acknowledges = """select cea.eventid , cea.message as message from (
        select eventid,MAX(cea.acknowledgeid) acknowledgeid
        from cassia_event_acknowledges cea group by eventid ) ceaa
        left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid"""
        # ACTUALIZAR NOMBRE
        self.stored_name_diagnostic_problems_origen_1 = 'sp_diagnostic_problems1'
        self.stored_name_get_connectivity_data_by_device = "sp_connectivityHost"
        self.query_statement_get_device_alineacion = """select DISTINCT device_id from metrics_template mt 
        inner join metric_group mg on mg.group_id =mt.group_id 
        where nickname ='Alineación'"""
        self.stored_name_get_aligment_report_data_by_device = "sp_alignmentReport_Host"
        self.stored_name_catalog_devices_types = 'sp_catDevice'
        self.stored_name_catalog_devices_brands = 'sp_catBrand'
        self.stored_name_catalog_metric = "sp_catMetric"
        self.stored_name_catalog_models = "sp_catModel"
        self.stored_name_get_towers = "sp_catTower"
        self.stored_name_get_host_downs = "sp_hostDown"
        # ACTUALIZAR NOMBRE
        self.query_get_host_downs_dependientes = """SELECT DISTINCT (hostid) from cassia_diagnostic_problems cdp 
where closed_at is null and depends_hostid is not null"""
        self.query_get_rfid_arcos_data_v2_gto = """SELECT m.Nombre as Municipio, a.Nombre as Arco, r.Descripcion,
r.Estado, a2.UltimaLectura,
cl.lecturas as Lecturas,
a.Longitud,a.Latitud,
r.Ip
FROM RFID r
INNER JOIN Arco a ON (a.IdArco =r.IdArco )
INNER JOIN Municipio m ON (a.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(second,-30,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
order by a.Longitud,a.Latitud"""
        self.query_get_rfid_arcos_data_v1 = """SELECT m.Nombre as Municipio, a.Nombre as Arco, r.Descripcion,
r.Estado, a2.UltimaLectura,
cl.lecturas as Lecturas,
a.Longitud, a.Latitud,
r.Ip
FROM RFID r
INNER JOIN ArcoRFID ar  ON(R.IdRFID=ar.IdRFID)
INNER JOIN Arco a ON(ar.IdArco=a.IdArco)
INNER JOIN ArcoMunicipio am ON(a.IdArco=am.IdArco)
INNER JOIN Municipio m ON(am.IdMunicipio=M.IdMunicipio)
LEFT JOIN Antena a2  On(r.IdRFID=a2.IdRFID)
LEFT JOIN(select lr.IdRFID, lr.IdAntena,
           COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(second, -30, getdate()) and getdate()
group by lr.IdRFID, lr.IdAntena) cl ON(r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
order by a.Longitud, a.Latitud"""
        self.query_statement_get_rfid_readings_global = """
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude 
FROM cassia_arch_traffic c where c.`date` 
between DATE_ADD(now(),INTERVAL -5 MINUTE) and NOW()  
group by c.latitude, c.longitude
"""
        self.query_get_rfid_acitve = """SELECT DISTINCT c.longitude ,c.latitude 
FROM cassia_arch_traffic c 
group by c.latitude, c.longitude"""
        self.query_statement_get_lpr_readings_global = """
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude 
FROM cassia_arch_traffic_lpr c where c.`date` 
between DATE_ADD(now(),INTERVAL -10 MINUTE) and NOW()  
group by c.latitude, c.longitude
"""
        self.query_get_lpr_acitve = """SELECT DISTINCT c.longitude ,c.latitude 
FROM cassia_arch_traffic_lpr c 
group by c.latitude, c.longitude"""
        self.stored_name_switch_connectivity = "sp_switchConnectivity"
        self.query_get_cassia_exception_agencies = "SELECT * FROM cassia_exception_agencies where deleted_at IS NULL"
        self.query_get_cassia_exceptions = "select * from cassia_exceptions"

    def builder_query_statement_get_metrics_template(self, tech_id, alineacion_id):
        self.query_statement_get_metrics_template = f"""select * from metrics_template mt where device_id ='{tech_id}' and group_id ='{alineacion_id}'"""
        return self.query_statement_get_metrics_template

    def builder_query_statement_get_cassia_event(self, eventid):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_event = f"""select cassia_arch_traffic_events_id,created_at  from cassia_arch_traffic_events p where cassia_arch_traffic_events_id ='{eventid}'"""
        return self.query_statement_get_cassia_event

    def builder_query_statement_get_cassia_event_2(self, eventid):
        self.query_statement_get_cassia_event_2 = f"""select cassia_arch_traffic_events_id,created_at  from cassia_arch_traffic_events_2 p where cassia_arch_traffic_events_id ='{eventid}'"""
        return self.query_statement_get_cassia_event_2

    def builder_query_statement_get_cassia_event_tickets(self, eventid, is_cassia_event):
        self.query_statement_get_cassia_event_tickets = f"select * from cassia_tickets where event_id ='{eventid}' and is_cassia_event={is_cassia_event}"
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
        # ACTUALIZAR NOMBRE
        self.query_statement_get_hots_cassia_alerts = f"""
select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message  from cassia_arch_traffic_events cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id
where cate.hostid ={hostid} order by cate.created_at desc limit 20
"""
        return self.query_statement_get_hots_cassia_alerts

    def builder_query_statement_get_local_events_diagnosta(self, hostid):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_local_events_diagnosta = f"select local_eventid from cassia_diagnostic_problems where hostid={hostid}"
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
        # ACTUALIZAR NOMBRE
        self.query_statement_get_arch_traffic_events_date_close_null_municipality_template = f"""select * from cassia_arch_traffic_events WHERE closed_at IS NULL and municipality ='{municipality}' and alert_type='rfid'"""
        return self.query_statement_get_arch_traffic_events_date_close_null_municipality_template

    def builder_query_statement_get_config_data_by_name(self, name):
        self.query_statement_get_config_data_by_name = f"""select * from cassia_config where name='{name}'"""
        return self.query_statement_get_config_data_by_name

    def builder_query_statement_get_data_problems(self, list_hosts_downs_origen_ids):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_data_problems_by_list_ids = f"""
            select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_arch_traffic_events cate
            left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
            from cassia_event_acknowledges cea group by eventid ) as ceaa
            on  cate.cassia_arch_traffic_events_id=ceaa.eventid
            left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
            left join cassia_diagnostic_problems cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id
            where cate.closed_at is NULL and cate.hostid in {list_hosts_downs_origen_ids}"""
        return self.query_statement_get_data_problems_by_list_ids

    def builder_query_statement_get_global_cassia_events_by_tech(self, tech_id):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_global_cassia_events_by_tech = f"""SELECT * FROM cassia_arch_traffic_events where closed_at is NULL and tech_id='{tech_id}'"""
        return self.query_statement_get_global_cassia_events_by_tech

    def builder_query_statement_get_cassia_events_by_tech_and_municipality(self, municipality, tech_id):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_events_by_tech_and_municipality = f"""
        SELECT * FROM cassia_arch_traffic_events where closed_at is NULL and tech_id={tech_id} and municipality='{municipality}'"""
        return self.query_statement_get_cassia_events_by_tech_and_municipality

    def builder_query_statement_get_cassia_events_with_hosts_filter(self, hostids):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_events_with_hosts_filter = f"""select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_arch_traffic_events cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id 
where cate.closed_at is NULL and cate.hostid in ({hostids})"""
        return self.query_statement_get_cassia_events_with_hosts_filter

    def builder_query_statement_get_pertenencia_host_metric(self, hostid, metricid):
        self.query_statement_get_pertenencia_host_metric = f"""select * from hosts h
        inner join host_inventory hi 
        on h.hostid =hi.hostid 
        where hi.hostid ={hostid}
        and hi.device_id ={metricid}"""
        return self.query_statement_get_pertenencia_host_metric

    def builder_query_statement_get_ci_element_by_id(self, element_id):
        self.query_statement_get_ci_element_by_id = f"SELECT * FROM cassia_ci_element where element_id={element_id} limit 1"
        return self.query_statement_get_ci_element_by_id

    def builder_query_statement_get_ci_element_docs_by_id(self, element_id):
        self.query_statement_get_ci_element_docs_by_id = f"SELECT * FROM cassia_ci_documents1 where element_id={element_id}"
        return self.query_statement_get_ci_element_docs_by_id

    def builder_query_statement_get_ci_element_doc_by_id(self, doc_id):
        self.query_statement_get_ci_element_doc_by_id = f"SELECT * FROM cassia_ci_documents1 where doc_id={doc_id} limit 1"
        return self.query_statement_get_ci_element_doc_by_id

    def builder_query_statement_delete_ci_element_doc_by_id(self, doc_id):
        self.query_statement_delete_ci_element_doc_by_id = f"DELETE FROM cassia_ci_documents1 where doc_id={doc_id}"
        return self.query_statement_delete_ci_element_doc_by_id

    def builder_query_statement_get_host_traffic_by_ip_v2_gto(self, ip):
        self.query_statement_get_host_traffic_by_ip_v2_gto = f"""
SELECT m.Nombre as Municipio, a.Nombre as Arco, r.Descripcion,
r.Estado, a2.UltimaLectura,
ISNULL(cl.lecturas,0)  as Lecturas,
a.Longitud,a.Latitud,
a2.Carril,
r.Ip 
FROM RFID r
--INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (r.IdArco =a.IdArco )
--INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (a.IdMunicipio =m.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
where r.Ip = '{ip}'
order by a.Longitud,a.Latitud"""

        return self.query_statement_get_host_traffic_by_ip_v2_gto

    def builder_query_statement_get_host_traffic_by_ip_v1(self, ip):
        self.query_statement_get_host_traffic_by_ip_v1 = f"""
SELECT m.Nombre as Municipio, a.Nombre as Arco, r.Descripcion,
r.Estado, a2.UltimaLectura,
ISNULL(cl.lecturas,0)  as Lecturas,
a.Longitud,a.Latitud,
a2.Carril, m.Nombre ,
r.Ip 
FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =m.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
where r.Ip = '{ip}'
order by a.Longitud,a.Latitud"""

        return self.query_statement_get_host_traffic_by_ip_v1

    def builder_query_statement_get_rfid_readings_by_municipality_name(self, municipality_name):
        self.query_statement_get_rfid_reading_by_municipality_name = f"""SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude FROM cassia_arch_traffic c 
where c.`date` between DATE_ADD(now(),INTERVAL -5 MINUTE) and NOW()  
and c.municipality  LIKE '{municipality_name}'
group by c.latitude, c.longitude """
        return self.query_statement_get_rfid_reading_by_municipality_name

    def builder_query_statement_get_lpr_readings_by_municipality_name(self, municipality_name):
        self.query_statement_get_lpr_reading_by_municipality_name = f"""SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude FROM cassia_arch_traffic_lpr c 
where c.`date` between DATE_ADD(now(),INTERVAL -10 MINUTE) and NOW()  
and c.municipality  LIKE '{municipality_name}'
group by c.latitude, c.longitude """
        return self.query_statement_get_lpr_reading_by_municipality_name

    def builder_query_statement_get_max_severities_by_tech(self, tech_id):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_max_severities_by_tech = f"""SELECT max(c.severity) as max_severity, c.longitude ,c.latitude FROM cassia_arch_traffic_events c 
WHERE c.closed_at is NULL 
AND tech_id='{tech_id}'
group by c.latitude, c.longitude """
        return self.query_statement_get_max_severities_by_tech

    def builder_query_statement_delete_cassia_ticket_by_id(self, ticket_id):
        self.query_statement_delete_cassia_ticket_by_id = f"DELETE FROM cassia_tickets where ticket_id={ticket_id}"
        return self.query_statement_delete_cassia_ticket_by_id

    def builder_query_statement_get_cassia_ticket_by_id(self, ticket_id):
        self.query_statement_get_cassia_ticket_by_id = f"SELECT * FROM cassia_tickets where ticket_id={ticket_id}"
        return self.query_statement_get_cassia_ticket_by_id

    def builder_query_statement_get_cassia_exception_agency_by_id(self, exception_agency_id):
        self.query_statement_get_cassia_exception_agency_by_id = f"SELECT * FROM cassia_exception_agencies where exception_agency_id={exception_agency_id}"
        return self.query_statement_get_cassia_exception_agency_by_id

    def builder_query_statement_update_cassia_exception_agency(self, exception_agency_id, name, date):
        self.query_statement_update_cassia_exception_agency = f"""update cassia_exception_agencies set name='{name}', updated_at='{date}' where exception_agency_id={exception_agency_id}"""
        return self.query_statement_update_cassia_exception_agency

    def builder_query_statement_logic_delete_cassia_exception_agency(self, exception_agency_id, date):
        self.query_statement_logic_delete_cassia_exception_agency = f"""update cassia_exception_agencies set deleted_at='{date}' where exception_agency_id={exception_agency_id}"""
        return self.query_statement_logic_delete_cassia_exception_agency

    def builder_query_statement_get_host_by_id(self, hostid):
        self.query_statement_get_host_by_id = f"""select * from hosts where hostid={hostid}"""
        return self.query_statement_get_host_by_id

    def builder_query_statement_get_exception_by_id(self, exception_id):
        self.query_statement_get_exception_by_id = f"""select * from cassia_exceptions where exception_id={exception_id}"""
        return self.query_statement_get_exception_by_id

    def builder_query_statement_close_exception_by_id(self, exception_id, date):
        self.query_statement_close_exception_by_id = f"""update cassia_exceptions set closed_at='{date}' where exception_id={exception_id}"""
        return self.query_statement_close_exception_by_id

    def builder_query_statement_close_event_by_id(self, event_id, date):
        # ACTUALIZAR NOMBRE
        self.query_statement_close_event_by_id = f"""update cassia_arch_traffic_events set closed_at='{date}',
          message='Evento cerrado manualmente',
          status='Cerrada manualmente'
          where cassia_arch_traffic_events_id={event_id}"""
        return self.query_statement_close_event_by_id
