from models.cassia_exceptions_async_test import CassiaExceptionsAsyncTest
from schemas import cassia_auto_action_condition_schema
from schemas import cassia_auto_action_schema
from schemas import cassia_technologies_schema
from schemas import cassia_tech_device_schema
from utils import traits


class DBQueries:

    def __init__(self):
        self.stored_name_get_connectivity_data_m = 'sp_connectivityM'
        self.stored_name_get_connectivity_data = 'sp_connectivity'
        self.query_statement_get_alineacion_group_id = "select group_id from metric_group mg where nickname = 'Alineación'"
        self.stored_name_get_aligment_report_data_m = 'sp_alignmentReportM'
        self.stored_name_get_aligment_report_data = 'sp_alignmentReport'
        self.stored_name_get_host_health_detail_data = 'sp_hostHealt'
        self.stored_name_get_host_view_data = 'sp_hostView'
        # PINK
        # CAMBIO BD
        self.stored_name_get_acknowledges = "sp_acknowledgeList1"
        self.stored_name_get_acknowledges_test = "sp_acknowledgeList1_test"
        # ACTUALIZAR NOMBRE
        # PINK
        # CAMBIO BD
        self.stored_name_get_dependents_diagnostic_problems = "sp_diagnostic_problemsD"
        self.stored_name_get_dependents_diagnostic_problems_test = "sp_diagnostic_problemsD_test"
        # ACTUALIZAR NOMBRE
        # PINK
        self.query_get_open_diagnosta_problems = "select * from cassia_diagnostic_problems cdp where cdp.closed_at is NULL"
        self.query_get_open_diagnosta_problems_test = "select * from cassia_diagnostic_problems_test cdp where cdp.closed_at is NULL"
        self.query_get_total_slack_notifications_count = "select count(notification_id) as notificaciones from cassia_slack_notifications"
        self.query_statement_get_metrics_template = None
        self.query_statement_get_host_correlation = None
        self.stored_name_problems_severity = 'sp_problembySev'
        # ACTUALIZAR NOMBRE
        self.query_statement_get_arch_traffic_events_date_close_null = "select * from cassia_arch_traffic_events WHERE closed_at IS NULL AND alert_type='rfid'"
        # PINK
        self.query_statement_get_arch_traffic_events_date_close_null_test = "select * from cassia_events_test WHERE closed_at IS NULL AND alert_type='rfid'"
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
        # PINK
        self.query_statement_get_cassia_events_acknowledges_test = """select cea.eventid , cea.message as message from (
        select eventid,MAX(cea.acknowledgeid) acknowledgeid
        from cassia_event_acknowledges_test cea group by eventid ) ceaa
        left join cassia_event_acknowledges_test cea on cea.acknowledgeid  =ceaa.acknowledgeid"""
        # ACTUALIZAR NOMBRE
        # PINK
        # CAMBIO BD
        self.stored_name_diagnostic_problems_origen_1 = 'sp_diagnostic_problems1'
        self.stored_name_diagnostic_problems_origen_1_test = 'sp_diagnostic_problems1_test'
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
        # PINK
        self.query_get_host_downs_dependientes_test = """SELECT DISTINCT (hostid) from cassia_diagnostic_problems_test cdp
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
        self.stored_name_get_cassia_exceptions = "sp_getExceptions_detail"
        # PINK
        # CAMBIO BD
        self.stored_name_get_cassia_exceptions_test = "sp_getExceptions_detail_test"
        self.query_close_auto_action_operation_values = "sp_getExceptions_detail"

        self.stored_name_exceptions_count = "sp_getExceptions"
        # PINK
        # CAMBIO BD
        self.stored_name_exceptions_count_test = "sp_getExceptions_test"

        self.query_get_cassia_report_names = """select crfs.cassia_report_frequency_schedule_id , crfs.name,crfs.description
from cassia_report_frequency_schedule crfs"""
        self.query_get_cassia_users = """select user_id,mail,name from cassia_users cu
where cu.deleted_at  is NULL"""
        self.query_get_cassia_user_reports = """select user_id,cassia_report_frequency_schedule_id from
cassia_user_reports cur"""
        self.query_get_hosts_actions_to_process = """select interface_id,is_auto,ia.action_id,condition_id,i.hostid, caa.action_auto_id
from interface_action ia inner join
cassia_action_auto caa on ia.action_id =caa.action_id
inner join interface i
on i.interfaceid=ia.interface_id
where ia.is_auto=1"""
        self.query_get_cassia_auto_operational_values_to_process = """select cao.*,i.ip  from cassia_auto_operational cao
inner join interface i on i.interfaceid =cao.interface_id
where cao.status='EnProceso'"""
        self.stored_name_get_credentials = "sp_getCredentials"
        self.query_get_auto_actions_conditions = "SELECT * FROM cassia_auto_condition"
        self.query_get_auto_actions = """select caa.*,cac.name as condition_name ,ca.name as cassia_action_name from
cassia_action_auto caa inner join cassia_action ca on ca.action_id =caa.action_id
inner join cassia_auto_condition cac on cac.condition_id =caa.condition_id"""
        """ self.query_get_cassia_technologies = "select * from cassia_technologies where deleted_at is NULL" """
        self.query_get_cassia_technologies = "select * from cassia_ci_tech"
        self.query_get_cassia_exceptions_detail = """SELECT ce.*,cea.name ,h.host, hi.location_lat ,hi.location_lon
FROM cassia_exceptions ce
INNER JOIN host_inventory hi ON hi.hostid =ce.hostid
inner join hosts h on h.hostid =ce.hostid
inner join cassia_exception_agencies cea
on cea.exception_agency_id = ce.exception_agency_id
WHERE closed_at is NULL """
        # PINK
        self.query_get_cassia_exceptions_detail_test = """SELECT ce.*,cea.name ,h.host, hi.location_lat ,hi.location_lon
FROM cassia_exceptions_test ce
INNER JOIN host_inventory hi ON hi.hostid =ce.hostid
inner join hosts h on h.hostid =ce.hostid
inner join cassia_exception_agencies cea
on cea.exception_agency_id = ce.exception_agency_id
WHERE ce.closed_at is NULL and ce.deleted_at is NULL"""
        self.query_get_events_config = "SELECT * FROM cassia_events_config"
        self.query_get_cassia_criticalities = "SELECT * FROM cassia_criticalities where deleted_at is NULL"
        self.query_get_cassia_tech_services = """select cts.*,cc.level as criticality_level FROM cassia_tech_services cts
        left join cassia_criticalities cc on cc.cassia_criticality_id=cts.cassia_criticality_id
        where cts.deleted_at is null
        """
        self.sp_get_host_validation_down = "sp_validationDown"

        # PINK
        self.query_statement_update_exception = None
        # PINK
        self.query_statement_delete_exception = None
        # PINK
        self.query_statement_delete_maintenance = None
        # PINK
        self.query_get_maintenances = """select 
                        m.maintenance_id, m.date_start, m.date_end, m.description, m.created_at, m.updated_at
                        from cassia_maintenance m inner join hosts h on m.hostid = h.hostid  WHERE m.deleted_at is null"""
        # PINK
        self.query_statement_get_maintenance_between_dates_and_id = None

        self.query_get_notification_types = """SELECT * FROM cassia_notification_types"""
        self.query_get_user_notification_types_old = """select DISTINCT user_id,cu.cassia_notification_type_id,cnt.name  from cassia_user_notification_types cu
inner join cassia_notification_types cnt on cnt.cassia_notification_type_id =cu.cassia_notification_type_id """
        self.query_get_user_notification_types = """select DISTINCT user_id ,cunt.cassia_notification_type_id,cnt.name  from cassia_user_notification_types cunt 
inner join cassia_notification_types cnt on cnt.cassia_notification_type_id =cunt.cassia_notification_type_id"""
        self.query_get_users = """select user_id, mail, name from cassia_users where deleted_at is NULL"""
        self.query_get_tech_names_with_service = """select ct.cassia_tech_id,ct.tech_name,cts.service_name,cts.cassia_tech_service_id from cassia_techs ct 
inner join cassia_tech_services cts 
on cts.cassia_tech_service_id =ct.service_id 
where ct.deleted_at  is NULL"""

        #RESETS
        self.query_get_resets = """SELECT reset_id, affiliation, object_id, updated_at, imei FROM cassia_reset"""

    def builder_query_statement_get_metrics_template(self, tech_id, alineacion_id):
        self.query_statement_get_metrics_template = f"""select * from metrics_template mt where device_id ='{tech_id}' and group_id ='{alineacion_id}'"""
        return self.query_statement_get_metrics_template

    def builder_query_statement_get_cassia_event(self, eventid):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_event = f"""select cassia_arch_traffic_events_id,created_at  from cassia_arch_traffic_events p where cassia_arch_traffic_events_id ='{eventid}'"""
        return self.query_statement_get_cassia_event

    # PINK

    def builder_query_statement_get_cassia_event_test(self, eventid):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_event = f"""select cassia_arch_traffic_events_id,created_at  from cassia_events_test p where cassia_arch_traffic_events_id ='{eventid}'"""
        return self.query_statement_get_cassia_event

    def builder_query_statement_get_cassia_event_2(self, eventid):
        self.query_statement_get_cassia_event_2 = f"""select cassia_arch_traffic_events_id,created_at  from cassia_arch_traffic_events_2 p where cassia_arch_traffic_events_id ='{eventid}'"""
        return self.query_statement_get_cassia_event_2

    def builder_query_statement_get_cassia_event_tickets(self, eventid, is_cassia_event):
        self.query_statement_get_cassia_event_tickets = f"select * from cassia_tickets where event_id ='{eventid}' and is_cassia_event={is_cassia_event}"
        return self.query_statement_get_cassia_event_tickets

    # PINK
    def builder_query_statement_get_cassia_event_tickets_test(self, eventid, is_cassia_event):
        self.query_statement_get_cassia_event_tickets = f"select * from cassia_tickets_test where event_id ='{eventid}' and is_cassia_event={is_cassia_event}"
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

    # TEST
    def builder_query_statement_get_hots_cassia_alerts_test(self, hostid):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_hots_cassia_alerts = f"""
select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message  from cassia_events_test cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges_test cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges_test cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_test cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id
where cate.hostid ={hostid} order by cate.created_at desc limit 20
"""
        return self.query_statement_get_hots_cassia_alerts

    def builder_query_statement_get_local_events_diagnosta(self, hostid):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_local_events_diagnosta = f"select local_eventid from cassia_diagnostic_problems where hostid={hostid}"
        return self.query_statement_get_local_events_diagnosta

    # PINK
    def builder_query_statement_get_local_events_diagnosta_test(self, hostid):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_local_events_diagnosta = f"select local_eventid from cassia_diagnostic_problems_test where hostid={hostid}"
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

    # PINK
    def builder_query_statement_get_arch_traffic_events_date_close_null_municipality_test(self, municipality):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_arch_traffic_events_date_close_null_municipality_template = f"""select * from cassia_events_test WHERE closed_at IS NULL and municipality ='{municipality}' and alert_type='rfid'"""
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

    # PINK
    def builder_query_statement_get_global_cassia_events_by_tech_test(self, tech_id):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_global_cassia_events_by_tech = f"""SELECT * FROM cassia_events_test where closed_at is NULL and tech_id='{tech_id}'"""
        return self.query_statement_get_global_cassia_events_by_tech

    def builder_query_statement_get_cassia_events_by_tech_and_municipality(self, municipality, tech_id):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_events_by_tech_and_municipality = f"""
        SELECT * FROM cassia_arch_traffic_events where closed_at is NULL and tech_id={tech_id} and municipality='{municipality}'"""
        return self.query_statement_get_cassia_events_by_tech_and_municipality

    # PINK
    def builder_query_statement_get_cassia_events_by_tech_and_municipality_test(self, municipality, tech_id):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_events_by_tech_and_municipality = f"""
        SELECT * FROM cassia_events_test where closed_at is NULL and tech_id={tech_id} and municipality='{municipality}'"""
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

    # PINK
    def builder_query_statement_get_cassia_events_with_hosts_filter_test(self, hostids):
        # ACTUALIZAR NOMBRE
        self.query_statement_get_cassia_events_with_hosts_filter = f"""select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_events_test cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges_test cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges_test cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_test cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id 
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

    # PINK
    def builder_query_statement_delete_cassia_ticket_by_id_test(self, ticket_id):
        self.query_statement_delete_cassia_ticket_by_id = f"DELETE FROM cassia_tickets_test where ticket_id={ticket_id}"
        return self.query_statement_delete_cassia_ticket_by_id

    def builder_query_statement_get_cassia_ticket_by_id(self, ticket_id):
        self.query_statement_get_cassia_ticket_by_id = f"SELECT * FROM cassia_tickets where ticket_id={ticket_id}"
        return self.query_statement_get_cassia_ticket_by_id

    # PINK
    def builder_query_statement_get_cassia_ticket_by_id_test(self, ticket_id):
        self.query_statement_get_cassia_ticket_by_id = f"SELECT * FROM cassia_tickets_tests where ticket_id={ticket_id}"
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

    # PINK

    def builder_query_statement_get_exception_by_id_test(self, exception_id):
        self.query_statement_get_exception_by_id = f"""select * from cassia_exceptions_test where exception_id={exception_id}"""
        return self.query_statement_get_exception_by_id

    def builder_query_statement_close_exception_by_id(self, exception_id, date):
        self.query_statement_close_exception_by_id = f"""update cassia_exceptions set closed_at='{date}' where exception_id={exception_id}"""
        return self.query_statement_close_exception_by_id

    # PINK
    def builder_query_statement_close_exception_by_id_test(self, exception_id, date):
        self.query_statement_close_exception_by_id = f"""update cassia_exceptions_test set closed_at='{date}' where exception_id={exception_id}"""
        return self.query_statement_close_exception_by_id

    def builder_query_statement_close_event_by_id(self, event_id, date):
        # ACTUALIZAR NOMBRE
        self.query_statement_close_event_by_id = f"""update cassia_arch_traffic_events set closed_at='{date}',
          message='Evento cerrado manualmente',
          status='Cerrada manualmente'
          where cassia_arch_traffic_events_id={event_id}"""
        return self.query_statement_close_event_by_id

    # PINK
    def builder_query_statement_close_event_by_id_test(self, event_id, date):
        self.query_statement_close_event_by_id = f"""update cassia_events_test set closed_at='{date}',
          message='Evento cerrado manualmente',
          status='Cerrada manualmente'
          where cassia_arch_traffic_events_id={event_id}"""
        return self.query_statement_close_event_by_id

    def builder_query_statement_get_zabbix_acks(self, eventids):
        self.query_statement_get_zabbix_acks = f"""
select eventid,message from acknowledges a where eventid in({eventids})
"""
        return self.query_statement_get_zabbix_acks

    def builder_query_statement_get_cassia_acks(self, eventids):
        self.query_statement_get_cassia_acks = f"""
select eventid,message from cassia_event_acknowledges cea  where eventid in({eventids})
"""
        return self.query_statement_get_cassia_acks

    # PINK
    def builder_query_statement_get_cassia_acks_test(self, eventids):
        self.query_statement_get_cassia_acks = f"""
select eventid,message from cassia_event_acknowledges_test cea  where eventid in({eventids})
"""
        return self.query_statement_get_cassia_acks

    def builder_query_statement_get_reports_to_process(self, lower_limit, upper_limit):
        self.query_statement_get_reports_to_process = f"""
        SELECT cr.internal_name, crf.monthly, crf.day_of_month ,crf.weekly,
crf.day_of_week, crf.daily, crf.`hour`  from cassia_report_frequency_schedule crfs 
inner join cassia_reports cr on cr.cassia_reports_id =crfs.cassia_report_id 
inner join cassia_report_frequencies crf  on 
crf.cassia_report_frequency_id =crfs.cassia_report_frequency_id 
where hour BETWEEN '{lower_limit}' and '{upper_limit}'        
"""
        return self.query_statement_get_reports_to_process

    def builder_query_get_user_reports_mail_by_schedule_id(self, schedule_id):
        self.query_get_user_reports_mail_by_schedule_id = f"""select cur.user_id,mail from cassia_user_reports cur 
inner join cassia_users cu on cu.user_id =cur.user_id 
where cur.cassia_report_frequency_schedule_id ={schedule_id}"""
        return self.query_get_user_reports_mail_by_schedule_id

    def builder_query_delete_user_reports(self, user_id):
        self.query_delete_user_reports = f"""DELETE FROM cassia_user_reports  where user_id={user_id}"""
        return self.query_delete_user_reports

    def builder_query_delete_users_reports(self, user_ids):
        values = ", ".join(
            [str(user_id) for user_id in user_ids])
        self.query_delete_user_reports = f"""DELETE FROM cassia_user_reports  where user_id in ({values})"""
        return self.query_delete_user_reports

    def builder_query_insert_user_reports(self, user_id, reports_id):
        values = ", ".join(
            [f"({user_id}, {report_id})" for report_id in reports_id])
        self.query_insert_user_reports = f"""
INSERT INTO cassia_user_reports(user_id, cassia_report_frequency_schedule_id)
VALUES
{values}
"""
        return self.query_insert_user_reports

    def builder_query_insert_user_reports_values(self, values):
        self.query_insert_user_reports_values = f"""
INSERT INTO cassia_user_reports(user_id, cassia_report_frequency_schedule_id)
VALUES
{values}
"""
        return self.query_insert_user_reports_values

    def builder_query_get_auto_action_conditions(self, condition_id):
        self.query_get_auto_action_conditions = f"""
select cond_detail_id ,delay, template_id, range_min, range_max, units
from cassia_auto_condition_detail cacd where 
condition_id ={condition_id}
"""
        return self.query_get_auto_action_conditions

    def builder_query_get_auto_action_operational_values(self, interface_id, templateid):
        self.query_get_auto_action_operational_values = f"""
select * from cassia_auto_operational cao 
where interface_id = {interface_id}
and action_auto_id = {templateid}
and status='EnProceso'
"""
        return self.query_get_auto_action_operational_values

    def builder_query_insert_action_auto_operational_values(self, values):
        self.query_insert_action_auto_operational_values = f"""
INSERT INTO cassia_auto_operational(interface_id,action_auto_id,delay,templateid,lastValue,started_at,closed_at,status)
VALUES
{values}
"""
        return self.query_insert_action_auto_operational_values

    def builder_query_update_action_auto_operational_values(self, auto_operation_id, last_value, updated_at):
        self.query_update_action_auto_operational_values = f"""
update cassia_auto_operational 
set lastValue ='{last_value}',
updated_at='{updated_at}'
where auto_operation_id ={auto_operation_id}
"""
        return self.query_update_action_auto_operational_values

    def builder_query_close_action_auto_operational_values(self, auto_operation_ids, closed_at):
        self.query_close_action_auto_operational_values = f"""
update cassia_auto_operational 
set closed_at ='{closed_at}',
status='Cancelado'
where auto_operation_id in {auto_operation_ids}
"""
        return self.query_close_action_auto_operational_values

    def builder_query_increase_retry_times_auto_action_operation_values(self, ids, num_retry_times, now_str):
        self.query_increase_retry_times_auto_action_operation_values = f"""
update cassia_auto_operational 
set last_retry_date ='{now_str}',
action_retry_times={num_retry_times}
where auto_operation_id in ({ids})
"""
        return self.query_increase_retry_times_auto_action_operation_values

    def builder_query_close_auto_action_operation_values(self, ids, now_str):
        self.query_close_auto_action_operation_values = f"""
update cassia_auto_operational 
set status ='Cerrado',
updated_at='{now_str}',
closed_at='{now_str}'
where auto_operation_id in ({ids})
"""
        return self.query_close_auto_action_operation_values

    def builder_query_cancel_auto_action_operation_values_max_retry_times(self, ids, now_str):
        self.query_cancel_auto_action_operation_values_max_retry_times = f"""
update cassia_auto_operational 
set status ='Cancelado',
updated_at='{now_str}',
closed_at='{now_str}'
where auto_operation_id in ({ids})
"""
        return self.query_cancel_auto_action_operation_values_max_retry_times

    def builder_query_get_auto_condition_by_id(self, condition_id):
        self.query_get_auto_condition_by_id = f"""SELECT * FROM cassia_auto_condition where condition_id={condition_id}"""
        return self.query_get_auto_condition_by_id

    def builder_query_get_auto_condition_detail_by_id(self, condition_id):
        self.query_get_auto_condition_detail_by_id = f"""SELECT * FROM cassia_auto_condition_detail where condition_id={condition_id}"""
        return self.query_get_auto_condition_detail_by_id

    def builder_query_get_auto_condition_detail_by_detail_id(self, condition_detail_id):
        self.query_get_auto_condition_detail_by_detail_id = f"""SELECT * FROM cassia_auto_condition_detail where cond_detail_id={condition_detail_id}"""
        return self.query_get_auto_condition_detail_by_detail_id

    def builder_delete_auto_condition_detail_by_id(self, condition_id):
        self.delete_auto_condition_detail_by_id = f"""DELETE FROM cassia_auto_condition_detail where condition_id={condition_id}"""
        return self.delete_auto_condition_detail_by_id

    def builder_delete_auto_condition_by_id(self, condition_id):
        self.delete_auto_condition_by_id = f"""DELETE FROM cassia_auto_condition where condition_id={condition_id}"""
        return self.delete_auto_condition_by_id

    def builder_query_get_auto_action_by_id(self, action_auto_id):
        self.query_get_auto_action_by_id = f"""select caa.*,cac.name as condition_name ,ca.name as cassia_action_name from 
cassia_action_auto caa inner join cassia_action ca on ca.action_id =caa.action_id 
inner join cassia_auto_condition cac on cac.condition_id =caa.condition_id
WHERE caa.action_auto_id={action_auto_id}"""
        return self.query_get_auto_action_by_id

    def builder_query_get_action_by_id(self, action_id):
        self.query_get_action_by_id = f"""select * from cassia_action where action_id={action_id}"""
        return self.query_get_action_by_id

    def builder_query_delete_action_auto_by_id(self, action_id):
        self.query_delete_action_auto_by_id = f"""DELETE FROM cassia_action_auto where action_auto_id={action_id}"""
        return self.query_delete_action_auto_by_id

    def builder_query_update_action_condition_by_id(self, condition_id, name):
        self.query_update_action_condition_by_id = f"""
        UPDATE cassia_auto_condition
        SET name='{name}'
        WHERE condition_id={condition_id}"""
        return self.query_update_action_condition_by_id

    def builder_query_update_action_condition_detail_by_id(self,
                                                           condition_detail_data: cassia_auto_action_condition_schema.AutoActionConditionDetailUpdateSchema):
        self.query_update_action_condition_detail_by_id = f"""
        UPDATE cassia_auto_condition_detail
        SET condition_id='{condition_detail_data.condition_id}',
        delay='{condition_detail_data.delay}',
        template_name='{condition_detail_data.template_name}',
        template_id={condition_detail_data.template_id},
        range_min='{condition_detail_data.range_min}',
        range_max='{condition_detail_data.range_max}',
        units='{condition_detail_data.units}'
        WHERE cond_detail_id={condition_detail_data.cond_detail_id}"""
        return self.query_update_action_condition_detail_by_id

    def builder_query_update_action_by_id(self, action_data: cassia_auto_action_schema.AutoActionUpdateSchema):
        self.query_update_action_by_id = f"""
        UPDATE cassia_action_auto
        SET name='{action_data.name}',
        description='{action_data.description}',
        action_id={action_data.action_id},
        type_trigger='{action_data.type_trigger}',
        condition_id={action_data.condition_id}
        WHERE action_auto_id={action_data.action_auto_id}"""
        return self.query_update_action_by_id

    def builder_query_get_cassia_technology_by_id(self, cassia_technology_id):
        self.query_get_cassia_technology_by_id = f"""select * from cassia_ci_tech where 
        tech_id={cassia_technology_id}"""
        return self.query_get_cassia_technology_by_id

    def builder_query_update_cassia_technology_by_id(self, cassia_technology_id,
                                                     tech_data: cassia_technologies_schema.CassiaTechnologySchema):
        self.query_update_cassia_technology_by_id = f"""
        UPDATE cassia_technologies
        SET technology_name='{tech_data.technology_name}',
        sla={tech_data.sla},
        tech_group_ids='{tech_data.tech_group_ids}',
        updated_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_technologies_id={cassia_technology_id}"""
        return self.query_update_cassia_technology_by_id

    def builder_query_delete_cassia_technology_by_id(self, cassia_technology_id):
        self.query_delete_cassia_technology_by_id = f"""
        UPDATE cassia_technologies
        SET deleted_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_technologies_id={cassia_technology_id}"""
        return self.query_delete_cassia_technology_by_id

    def builder_query_get_technology_devices_by_ids(self, tech_group_ids):
        self.query_get_technology_devices_by_ids = f"""
        select h.hostid ,h.host,h.name,hi.alias,hi.location,hi.location_lat ,hi.location_lon,hi.device_id  from hosts h 
inner join host_inventory hi on hi.hostid =h.hostid 
where device_id  in ({tech_group_ids})"""
        return self.query_get_technology_devices_by_ids

    def builder_query_get_technology_devices_by_tech_id(self, tech_id):
        self.query_get_technology_devices_by_tech_id = f"""SELECT 
    cce.element_id,
    cce.host_id ,
    cce.ip,
    h.name,
    cce.technology,
    cce.device_name,
    cce.description,
    cce.referencia,
    his.hardware_brand,
    his.hardware_model,
    his.software_version,
    his.hardware_no_serie,
    cct.tech_name 
FROM 
    cassia_ci_element cce
LEFT JOIN 
    hosts h ON h.hostid = cce.host_id
LEFT JOIN 
    cassia_ci_tech cct ON cct.tech_id = cce.tech_id
LEFT JOIN 
    (
        SELECT 
            cch.element_id,
            cch.hardware_brand,
            cch.hardware_model,
            cch.software_version,
            cch.hardware_no_serie 
        FROM 
            (
                SELECT 
                    element_id,
                    hardware_brand,
                    hardware_model,
                    software_version,
                    hardware_no_serie,
                    ROW_NUMBER() OVER (PARTITION BY element_id ORDER BY closed_at DESC) AS rn
                FROM 
                    cassia_ci_history
                WHERE 
                    status = "Cerrada" 
                    AND deleted_at IS NULL
            ) cch
        WHERE 
            rn = 1
    ) his ON cce.element_id = his.element_id
WHERE 
    cce.deleted_at IS NULL
    and cce.tech_id={tech_id}
    """
        return self.query_get_technology_devices_by_tech_id

    def builder_query_get_cassia_criticality_by_id(self, cassia_criticality_id):
        self.query_get_cassia_criticality_by_id = f"""select * from cassia_criticalities where 
        cassia_criticality_id={cassia_criticality_id} and deleted_at is NULL"""
        return self.query_get_cassia_criticality_by_id

    def builder_query_delete_cassia_criticality_by_id(self, cassia_criticality_id):
        self.query_delete_cassia_criticality_by_id = f"""
        UPDATE cassia_criticalities
        SET deleted_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_criticality_id={cassia_criticality_id}"""
        return self.query_delete_cassia_criticality_by_id

    def builder_query_update_cassia_criticality_by_id(self, cassia_criticality_id, criticality_data, file_dest, filename):
        self.query_update_cassia_criticality_by_id = f"""
        UPDATE cassia_criticalities
        SET level={criticality_data.level},
        name='{criticality_data.name}',
        description='{criticality_data.description}',
        icon='{file_dest}',
         filename='{filename}' WHERE cassia_criticality_id={cassia_criticality_id}
""" if file_dest else f"""
        UPDATE cassia_criticalities
        SET level={criticality_data.level},
        name='{criticality_data.name}',
        description='{criticality_data.description}' WHERE cassia_criticality_id={cassia_criticality_id}
"""
        return self.query_update_cassia_criticality_by_id

    def builder_query_get_cassia_service_tech_by_id(self, cassia_service_tech_id):
        self.query_get_cassia_service_tech_by_id = f"""select cts.*,cc.level as criticality_level from cassia_tech_services cts 
        left join cassia_criticalities cc on cc.cassia_criticality_id= cts.cassia_criticality_id where 
        cassia_tech_service_id={cassia_service_tech_id} and cts.deleted_at is null"""
        return self.query_get_cassia_service_tech_by_id

    def builder_query_update_cassia_service_tech_by_id(self, cassia_service_tech_id, service_data):
        self.query_update_cassia_service_tech_by_id = f"""
        UPDATE cassia_tech_services
        SET service_name='{service_data.service_name}',
         description='{service_data.description}',
          cassia_criticality_id={service_data.cassia_criticality_id},
           updated_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_tech_service_id={cassia_service_tech_id}""" if service_data.cassia_criticality_id is not None else f"""
        UPDATE cassia_tech_services
        SET service_name='{service_data.service_name}',
         description='{service_data.description}',
          cassia_criticality_id=Null,
           updated_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_tech_service_id={cassia_service_tech_id}"""
        return self.query_update_cassia_service_tech_by_id

    def builder_query_delete_cassia_tech_service_by_id(self, cassia_tech_service_id):
        self.query_delete_cassia_tech_service_by_id = f"""
        UPDATE cassia_tech_services
        SET deleted_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_tech_service_id={cassia_tech_service_id}"""
        return self.query_delete_cassia_tech_service_by_id

    def builder_query_get_cassia_criticality_level(self, level):
        self.query_get_cassia_criticality_level = f"""
        SELECT * FROM cassia_criticalities where
        level={level} AND deleted_at is NULL"""
        return self.query_get_cassia_criticality_level

    def builder_query_get_techs_by_service_id(self, service_id):
        self.query_get_techs_by_service_id = f"""
        SELECT ct.*,cc.level as criticality_level FROM cassia_techs ct left join 
        cassia_criticalities cc on cc.cassia_criticality_id=ct.cassia_criticality_id
        where
        service_id={service_id} AND ct.deleted_at is NULL
        """
        return self.query_get_techs_by_service_id

    def builder_query_get_cassia_tech_by_id(self, tech_id):
        self.query_get_cassia_tech_by_id = f"""select ct.*,cc.level as criticality_level from cassia_techs ct 
        left join cassia_criticalities cc on cc.cassia_criticality_id=ct.cassia_criticality_id
        where
        cassia_tech_id={tech_id} and ct.deleted_at is NULL"""
        return self.query_get_cassia_tech_by_id

    def builder_query_update_cassia_tech_by_id(self, cassia_tech_id, tech_data):
        self.query_update_cassia_tech_by_id = f"""
        UPDATE cassia_techs
        SET tech_name='{tech_data.tech_name}',
         tech_description='{tech_data.tech_description}',
         service_id={tech_data.service_id},
          cassia_criticality_id={tech_data.cassia_criticality_id},
          sla_hours={tech_data.sla_hours},
           updated_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_tech_id={cassia_tech_id}""" if tech_data.cassia_criticality_id is not None else f"""
        UPDATE cassia_techs
        SET tech_name='{tech_data.tech_name}',
         tech_description='{tech_data.tech_description}',
         service_id={tech_data.service_id},
          cassia_criticality_id=Null,
          sla_hours={tech_data.sla_hours},
           updated_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_tech_id={cassia_tech_id}"""
        return self.query_update_cassia_tech_by_id

    def builder_query_delete_cassia_tech_by_id(self, cassia_tech_id):
        self.query_delete_cassia_tech_by_id = f"""
        UPDATE cassia_techs
        SET deleted_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_tech_id={cassia_tech_id}"""
        return self.query_delete_cassia_tech_by_id

    def builder_query_get_devices_by_tech_id(self, tech_id):
        self.query_get_devices_by_tech_id = f"""
select ctd.cassia_tech_device_id,h.hostid ,i.ip,h.host,h.name,hi.alias,hi.location,hi.location_lat ,hi.location_lon,hi.device_id,ctd.criticality_id,cc.level as criticality_level  from hosts h 
inner join host_inventory hi on hi.hostid =h.hostid 
inner join cassia_tech_devices ctd on ctd.hostid =h.hostid 
inner join (select DISTINCT hostid,ip from interface i) i on i.hostid =h.hostid 
left join cassia_criticalities cc on cc.cassia_criticality_id = ctd.criticality_id
where ctd.cassia_tech_id ={tech_id}
and ctd.deleted_at is null"""
        return self.query_get_devices_by_tech_id

    # PINK
    def builder_query_statement_update_exception(self, exception_data: CassiaExceptionsAsyncTest):
        self.query_statement_update_exception = f"""update cassia_exceptions_test set 
                                                    exception_agency_id={exception_data.exception_agency_id},  
                                                    description='{exception_data.description}', 
                                                    session_id
                                                    closed_at='{exception_data}' 
                                                    where exception_id={exception_data.exception_id}"""
        return self.query_statement_close_exception_by_id

    def builder_query_statement_logic_delete_cassia_exception(self, exception_id, date):
        self.query_statement_delete_exception = f"""update cassia_exceptions_test set 
                                                            deleted_at='{date}'  
                                                            where exception_id={exception_id}"""
        return self.query_statement_delete_exception

    def builder_query_update_cassia_tech_device_by_id(self, cassia_tech_device_id, device_data: cassia_tech_device_schema):
        self.query_update_cassia_tech_device_by_id = f"""
        UPDATE cassia_tech_devices
        SET criticality_id={device_data.criticality_id},
         hostid={device_data.hostid},
         cassia_tech_id={device_data.cassia_tech_id},
         updated_at='{traits.get_datetime_now_str_with_tz()}'
    WHERE cassia_tech_device_id={cassia_tech_device_id}""" if device_data.criticality_id is not None else f"""
        UPDATE cassia_tech_devices
        SET criticality_id=Null,
         hostid={device_data.hostid},
         cassia_tech_id={device_data.cassia_tech_id},
         updated_at='{traits.get_datetime_now_str_with_tz()}'
    WHERE cassia_tech_device_id={cassia_tech_device_id}"""
        return self.query_update_cassia_tech_device_by_id

    def builder_query_get_tech_device_by_id(self, device_id):
        self.query_get_tech_device_by_id = f"""
select h.hostid ,i.ip,h.host,h.name,hi.alias,hi.location,hi.location_lat ,hi.location_lon,hi.device_id,ctd.criticality_id,cc.level as criticality_level  from hosts h 
inner join host_inventory hi on hi.hostid =h.hostid 
inner join cassia_tech_devices ctd on ctd.hostid =h.hostid 
inner join (select DISTINCT hostid,ip from interface i) i on i.hostid =h.hostid 
left join cassia_criticalities cc on cc.cassia_criticality_id = ctd.criticality_id
where ctd.cassia_tech_device_id={device_id}
and ctd.deleted_at is NULL"""
        return self.query_get_tech_device_by_id

    def builder_query_delete_cassia_tech_device_by_ids(self, device_ids):
        self.query_delete_cassia_tech_device_by_ids = f"""
        UPDATE cassia_tech_devices
        SET deleted_at='{traits.get_datetime_now_str_with_tz()}'
        WHERE cassia_tech_device_id in ({device_ids})"""
        return self.query_delete_cassia_tech_device_by_ids

    def builder_query_get_created_devices_by_ids(self, hostids):
        self.query_get_created_devices_by_ids = f"""
        SELECT * FROM cassia_tech_devices
        where hostid in ({hostids}) and deleted_at is null"""
        return self.query_get_created_devices_by_ids

    def builder_query_statement_get_maintenance_between_dates_and_id(self, host_id, date_start, date_end):
        self.query_statement_get_maintenance_between_dates_and_id = f"""
        SELECT * FROM cassia_maintenance 
        WHERE hostid = {host_id} 
        AND (date_start BETWEEN '{date_start}' AND '{date_end}' 
        OR date_end BETWEEN '{date_start}' AND '{date_end}');
        """
        return self.query_statement_get_maintenance_between_dates_and_id

    def builder_query_statement_logic_delete_maintenance(self, maintenance_id, date, current_user_session):
        self.query_statement_delete_maintenance = f"""update cassia_maintenance set 
                                                            deleted_at='{date}',
                                                            session_id='{current_user_session}'
                                                            where maintenance_id={maintenance_id}"""
        return self.query_statement_delete_maintenance

    def builder_query_get_cassia_service_techs_by_name(self, service_id, tech_name):
        self.query_get_cassia_service_techs_by_name = f"""
        SELECT * FROM cassia_techs where service_id={service_id}
and LOWER(tech_name) like '{tech_name}' and deleted_at IS NULL"""
        return self.query_get_cassia_service_techs_by_name

    def builder_query_get_down_events_by_hostids(self, hostids):
        self.query_get_down_events_by_hostids = f"""
        SELECT * FROM cassia_events_test
        where hostid in ({hostids}) and closed_at is null
        and alert_type='down'"""
        return self.query_get_down_events_by_hostids

    def builder_query_get_tech_ids_by_service_id(self, service_id):
        self.query_get_tech_ids_by_service_id = f"""
        SELECT * FROM cassia_techs
        where service_id={service_id} and deleted_at is null"""
        return self.query_get_tech_ids_by_service_id

    def builder_query_statement_get_user(self, user_id):
        self.query_statement_get_user = f"""SELECT * FROM cassia_users where user_id={user_id} and deleted_at IS NULL"""
        return self.query_statement_get_user

    def builder_query_statement_get_user_notifications_techs(self, user_id):
        self.query_statement_get_user_notifications_techs = f"""select DISTINCT cu.cassia_notification_type_id,cnt.name ,
cu.cassia_tech_id,ct.tech_name 
from cassia_user_notification_types_techs cu 
left join cassia_techs ct  
on ct.cassia_tech_id =cu.cassia_tech_id 
inner join cassia_notification_types cnt 
on cnt.cassia_notification_type_id = cu.cassia_notification_type_id 
where cu.user_id ={user_id}
and cu.cassia_notification_type_id in
(SELECT cassia_notification_type_id from cassia_user_notification_types cunt
where cunt.user_id={user_id})"""
        return self.query_statement_get_user_notifications_techs

    def builder_query_insert_user_notification_types(self, user_id, cassia_notification_type_id, techs_list):
        values = ", ".join(
            [f"({user_id}, {cassia_notification_type_id},{tech_id})" for tech_id in techs_list])
        self.query_insert_user_notification_types = f"""
INSERT INTO cassia_user_notification_types(user_id, cassia_notification_type_id,cassia_tech_id)
VALUES
{values}
"""
        return self.query_insert_user_notification_types

    def builder_query_statement_delete_user_notification_types_by_user_id(self, user_id):
        self.query_statement_delete_user_notification_types_by_user_id = f"""DELETE FROM cassia_user_notification_types where user_id={user_id}"""
        return self.query_statement_delete_user_notification_types_by_user_id

    def builder_query_statement_get_host_data_gs_ticket_by_host_id(self, host_id):
        self.query_statement_get_host_data_gs_ticket_by_host_id = f"""
select h.hostid ,h.host ,hi.alias , cch.software_version,cch.hardware_no_serie from hosts h 
inner join host_inventory hi on hi.hostid =h.hostid 
left join cassia_ci_element cce on cce.host_id =h.hostid 
left join (select cch.element_id, software_version,hardware_no_serie  from cassia_ci_history cch 
inner join cassia_ci_element cce2 on cce2.element_id = cch.element_id
where cce2.host_id  ={host_id} and cch.status ="Cerrada" order by closed_at desc 
limit 1) cch on cch.element_id =cce.element_id 
where host_id ={host_id}"""
        return self.query_statement_get_host_data_gs_ticket_by_host_id

    def builder_query_statement_get_active_tickets_by_afiliation(self, afiliacion):
        self.query_statement_get_active_tickets_by_afiliation = f"""
select * from cassia_gs_tickets cgt 
where cgt.afiliacion ='{afiliacion}'
and status !='cerrado'"""
        return self.query_statement_get_active_tickets_by_afiliation

    def builder_query_statement_get_ticket_by_ticket_id(self, ticket_id):
        self.query_statement_get_ticket_by_ticket_id = f"""
select * from cassia_gs_tickets cgt 
where cgt.ticket_id ={ticket_id}
and status !='cerrado'
and status !='procesando'"""
        return self.query_statement_get_ticket_by_ticket_id

    def builder_query_statement_delete_users_notifications_types_by_user_ids(self, user_ids):
        user_ids = ", ".join([str(user_id) for user_id in user_ids]) if len(
            user_ids) > 0 else 0
        self.query_statement_delete_users_notifications_types_by_user_ids = f"""DELETE FROM cassia_user_notification_types where user_id in ({user_ids})"""
        return self.query_statement_delete_users_notifications_types_by_user_ids

    def builder_query_insert_users_notification_types(self, values):
        self.query_insert_users_notification_types = f"""
        INSERT INTO cassia_user_notification_types (user_id,cassia_notification_type_id) values {values}"""

        return self.query_insert_users_notification_types
