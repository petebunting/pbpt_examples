from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import os.path
import logging
import sqlite3
import statistics
import os
from arcsilib.arcsiscnprocessdb import RecordScn2Process


logger = logging.getLogger(__name__)


def gen_arcsi_ls_sensor(spacecraft, sensor):
    arcsi_sensor = ''
    if spacecraft == 'LANDSAT_1':
        arcsi_sensor = 'ls1'
    elif spacecraft == 'LANDSAT_2':
        arcsi_sensor = 'ls2'
    elif spacecraft == 'LANDSAT_3':
        arcsi_sensor = 'ls4'
    elif (spacecraft == 'LANDSAT_4') and (sensor == 'MSS'):
        arcsi_sensor = 'ls4mss'
    elif (spacecraft == 'LANDSAT_5') and (sensor == 'MSS'):
        arcsi_sensor = 'ls5mss'
    elif (spacecraft == 'LANDSAT_4') and (sensor == 'TM'):
        arcsi_sensor = 'ls4tm'
    elif (spacecraft == 'LANDSAT_5') and (sensor == 'TM'):
        arcsi_sensor = 'ls5tm'
    elif spacecraft == 'LANDSAT_7':
        arcsi_sensor = 'ls7'
    elif spacecraft == 'LANDSAT_8':
        arcsi_sensor = 'ls8'
    else:
        raise Exception("Do not recognise the spacecraft/sensor.")
    return arcsi_sensor

class FindLandsatScnsGenDwnlds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        rowpath_lst = rsgis_utils.readTextFile2List(kwargs['rowpath_lst'])
        gg_ls_db_conn = sqlite3.connect(kwargs['db_file'])
        query = """SELECT PRODUCT_ID, SPACECRAFT_ID, SENSOR_ID, BASE_URL FROM LANDSAT WHERE WRS_ROW = ? AND WRS_PATH = ?
                   AND COLLECTION_NUMBER = ? AND CLOUD_COVER < ? 
                   AND date(SENSING_TIME) > date(?) AND date(SENSING_TIME) < date(?)  
                   ORDER BY CLOUD_COVER ASC, date(SENSING_TIME) DESC LIMIT {}""".format(kwargs['n_scns'] + kwargs['n_scns_xt'])
                   

        scn_rcd_obj = RecordScn2Process(kwargs['scn_db_file'])
        if not os.path.exists(kwargs['scn_db_file']):
            scn_rcd_obj.init_db()

        for rowpathstr in rowpath_lst:
            rowpath_lst = rowpathstr.split(',')
            row = int(rowpath_lst[0])
            path = int(rowpath_lst[1])
            logger.info("Processing Row/Path: [{}, {}]".format(row, path))
            rowpath_geoid = "r{}_p{}".format(row, path)
            n_scns = scn_rcd_obj.n_geoid_scns(rowpath_geoid)
            if n_scns < kwargs['n_scns']:
                gg_ls_db_cursor = gg_ls_db_conn.cursor()
                query_vars = [row, path, kwargs['collection'], kwargs['cloud_thres'], kwargs['start_date'], kwargs['end_date']]

                scn_ids = list()
                scn_lst = list()
                for row in gg_ls_db_cursor.execute(query, query_vars):
                    spacecraft_id = row[1]
                    sensor_id = row[2]
                    arcsi_sensor = gen_arcsi_ls_sensor(spacecraft_id, sensor_id)
                
                    if (not scn_rcd_obj.is_scn_in_db(row[0], arcsi_sensor)) and (row[0] not in scn_ids):
                        logger.info("Adding to processing: {}".format(row[0]))
                        scn = dict()
                        scn['product_id'] = row[0]
                        scn['sensor'] = arcsi_sensor
                        scn['scn_url'] = row[3]
                        scn['geo_str_id'] = rowpath_geoid
                        scn_lst.append(scn)

                        c_dict = dict()
                        c_dict['product_id'] = row[0]
                        c_dict['sensor'] = arcsi_sensor
                        c_dict['scn_url'] = row[3]
                        c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], row[0])
                        c_dict['scn_db_file'] = kwargs['scn_db_file']
                        c_dict['goog_key_json'] = kwargs['goog_key_json']
                        if not os.path.exists(c_dict['downpath']):
                            os.mkdir(c_dict['downpath'])
                        self.params.append(c_dict)
                        scn_ids.append(row[0])
                        n_scns += 1
                    elif not scn_rcd_obj.is_scn_downloaded(row[0], arcsi_sensor):
                        c_dict = dict()
                        c_dict['product_id'] = row[0]
                        c_dict['sensor'] = arcsi_sensor
                        c_dict['scn_url'] = row[3]
                        c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], row[0])
                        c_dict['scn_db_file'] = kwargs['scn_db_file']
                        c_dict['goog_key_json'] = kwargs['goog_key_json']
                        if not os.path.exists(c_dict['downpath']):
                            os.mkdir(c_dict['downpath'])
                        self.params.append(c_dict)
                    if n_scns >= kwargs['n_scns']:
                        break
                if len(scn_lst) > 0:
                    scn_rcd_obj.add_scns(scn_lst)
            else:
                #GET SCENES WHICH HAVE NOT DOWNLOADED AND ADD to JOB LIST.
                scns = scn_rcd_obj.get_scns_download(rowpath_geoid)
                for scn in scns:
                    c_dict = dict()
                    c_dict['product_id'] = scn.product_id
                    c_dict['sensor'] = scn.sensor_id
                    c_dict['scn_url'] = scn.scn_url
                    c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], scn.product_id)
                    c_dict['scn_db_file'] = kwargs['scn_db_file']
                    c_dict['goog_key_json'] = kwargs['goog_key_json']
                    if not os.path.exists(c_dict['downpath']):
                        os.mkdir(c_dict['downpath'])
                    self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(
            db_file='./ls_db_20200810.db',
            rowpath_lst='./rowpaths.txt',
            cloud_thres=20,
            collection = '01',
            start_date='2016-01-01',
            end_date='2020-07-01',
            n_scns=1,
            n_scns_xt=10,
            scn_db_file='./ls_scn.db',
            dwnld_path='/Users/pete/Temp/arcsi_test_db_class/ls_dwnlds',
            goog_key_json='/Users/pete/Temp/arcsi_test_db_class/GlobalMangroveWatch-74b58b05fd73.json')

        self.pop_params_db()
        self.create_shell_exe("run_dwnld_cmds.sh", "dwnld_cmds.sh", 4, db_info_file=None)
        #self.create_slurm_sub_sh("dwnld_ls_scns", 8224, '/scratch/a.pfb/gmw_v2_gapfill/logs',
        #                         run_script='run_exe_analysis.sh', job_dir="job_scripts",
        #                         db_info_file=None, account_name='scw1376', n_cores_per_job=5, n_jobs=2,
        #                         job_time_limit='2-23:59',
        #                         module_load='module load parallel singularity\n\n")

    def run_check_outputs(self):
        process_tools_mod = 'perform_dwnld_jobs'
        process_tools_cls = 'PerformScnDownload'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)

    def run_remove_outputs(self, all_jobs=False, error_jobs=False):
        process_tools_mod = 'perform_dwnld_jobs'
        process_tools_cls = 'PerformScnDownload'
        self.remove_job_outputs(process_tools_mod, process_tools_cls, all_jobs, error_jobs)

if __name__ == "__main__":
    py_script = os.path.abspath("perform_dwnld_jobs.py")
    #script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)
    script_cmd = "python {}".format(py_script)

    create_tools = FindLandsatScnsGenDwnlds(cmd=script_cmd, sqlite_db_file="dwnld_ls_scns.db")
    create_tools.parse_cmds()
