

class DBQueries():

    def __init__(self):
        self.stored_name_get_connectivity_data_m = 'sp_connectivityM'
        self.stored_name_get_connectivity_data = 'sp_connectivity'
        self.query_statement_get_alineacion_group_id = "select group_id from metric_group mg where nickname = 'Alineación'"
        self.stored_name_get_aligment_report_data_m = 'sp_alignmentReportM'
        self.stored_name_get_aligment_report_data = 'sp_alignmentReport'
        self.stored_name_get_host_health_detail_data = 'sp_hostHealt'
        self.query_statement_get_switch_config_data = "select * from cassia_config where name='switch_id'"
        self.query_statement_get_switch_throughput_config_data = "select * from cassia_config where name='switch_throughtput'"
        self.query_statement_get_rfid_config_data = "select * from cassia_config where name='rfid_id'"
        self.stored_name_get_host_view_data = 'sp_hostView'


    def builder_query_statement_get_metrics_template(self, tech_id, alineacion_id):
        self.query_statement_get_metrics_template = f"""select * from metrics_template mt where device_id ='{tech_id}' and group_id ='{alineacion_id}'"""
        return self.query_statement_get_metrics_template