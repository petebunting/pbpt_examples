from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import rsgislib
import os.path
import logging
import sqlite3
import statistics
import os
from arcsilib.arcsiscnprocessdb import RecordScn2Process


logger = logging.getLogger(__name__)

class FindSen2ScnsGenDwnlds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        granule_lst = rsgis_utils.readTextFile2List(kwargs['granule_lst'])
        gg_sen2_db_conn = sqlite3.connect(kwargs['db_file'])
        query = """SELECT PRODUCT_ID, BASE_URL FROM SEN2 WHERE MGRS_TILE = ? AND CLOUD_COVER < ? 
                   AND date(SENSING_TIME) > date(?) AND date(SENSING_TIME) < date(?) 
                   AND GEOMETRIC_QUALITY_FLAG = 0 AND CAST(TOTAL_SIZE as decimal) > ? 
                   ORDER BY CLOUD_COVER ASC, date(GENERATION_TIME) DESC LIMIT {}""".format(kwargs['n_scns'] + kwargs['n_scns_xt'])

        query_total_size = """SELECT TOTAL_SIZE FROM SEN2 WHERE MGRS_TILE = ? AND CLOUD_COVER < ? 
                              AND date(SENSING_TIME) > date(?) AND date(SENSING_TIME) < date(?)"""

        scn_rcd_obj = RecordScn2Process(kwargs['scn_db_file'])
        if not os.path.exists(kwargs['scn_db_file']):
            scn_rcd_obj.init_db()

        for granule in granule_lst:
            logger.info("Processing Granule: {}".format(granule))
            n_scns = scn_rcd_obj.n_geoid_scns(granule)
            if n_scns < kwargs['n_scns']:
                gg_sen2_db_cursor = gg_sen2_db_conn.cursor()
                query_ts_vars = [granule, kwargs['cloud_thres_ts'], kwargs['start_date'], kwargs['end_date']]
                total_size_lst = list()
                for row in gg_sen2_db_cursor.execute(query_total_size, query_ts_vars):
                    if rsgis_utils.isNumber(row[0]):
                        total_size_lst.append(float(row[0]))
                if len(total_size_lst) > 0:
                    if len(total_size_lst) > 1:
                        ts_mean = statistics.mean(total_size_lst)
                        ts_stdev = statistics.stdev(total_size_lst)
                        ts_thres = ts_mean - ts_stdev
                    else:
                        ts_thres = 0.0
                    logger.debug("Total Size Threshold: {}".format(ts_thres))
                    query_vars = [granule, kwargs['cloud_thres'], kwargs['start_date'], kwargs['end_date'], ts_thres]
                    scn_ids = list()
                    scn_lst = list()
                    for row in gg_sen2_db_cursor.execute(query, query_vars):
                        if (not scn_rcd_obj.is_scn_in_db(row[0], 'sen2')) and ("OPER_PRD" not in row[0]) and (row[0] not in scn_ids):
                            logger.info("Adding to processing: {}".format(row[0]))
                            scn = dict()
                            scn['product_id'] = row[0]
                            scn['sensor'] = 'sen2'
                            scn['scn_url'] = row[1]
                            scn['geo_str_id'] = granule
                            scn_lst.append(scn)

                            c_dict = dict()
                            c_dict['product_id'] = row[0]
                            c_dict['sensor'] = 'sen2'
                            c_dict['scn_url'] = row[1]
                            c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], row[0])
                            c_dict['scn_db_file'] = kwargs['scn_db_file']
                            c_dict['goog_key_json'] = kwargs['goog_key_json']
                            if not os.path.exists(c_dict['downpath']):
                                os.mkdir(c_dict['downpath'])
                            self.params.append(c_dict)
                            scn_ids.append(row[0])
                            n_scns += 1
                        elif not scn_rcd_obj.is_scn_downloaded(row[0], 'sen2'):
                            c_dict = dict()
                            c_dict['product_id'] = row[0]
                            c_dict['sensor'] = 'sen2'
                            c_dict['scn_url'] = row[1]
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
                scns = scn_rcd_obj.get_scns_download(granule)
                for scn in scns:
                    c_dict = dict()
                    c_dict['product_id'] = scn.product_id
                    c_dict['sensor'] = 'sen2'
                    c_dict['scn_url'] = scn.scn_url
                    c_dict['downpath'] = os.path.join(kwargs['dwnld_path'], scn.product_id)
                    c_dict['scn_db_file'] = kwargs['scn_db_file']
                    c_dict['goog_key_json'] = kwargs['goog_key_json']
                    if not os.path.exists(c_dict['downpath']):
                        os.mkdir(c_dict['downpath'])
                    self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(
            db_file='./sen2_db_20200701.db',
            granule_lst='./sen2_roi_granule_lst.txt',
            cloud_thres=20,
            cloud_thres_ts=50,
            start_date='2016-01-01',
            end_date='2020-07-01',
            n_scns=1,
            n_scns_xt=10,
            scn_db_file='./sen2_scn.db',
            dwnld_path='/Users/pete/Temp/arcsi_test_db_class/sen2_dwnlds',
            goog_key_json='/Users/pete/Temp/arcsi_test_db_class/GlobalMangroveWatch-74b58b05fd73.json')

        self.pop_params_db()
        self.create_shell_exe("run_dwnld_cmds.sh", "dwnld_cmds.sh", 4, db_info_file=None)
        #self.create_slurm_sub_sh("dwnld_sen2_scns", 8224, '/scratch/a.pfb/gmw_v2_gapfill/logs',
        #                         run_script='run_exe_analysis.sh', job_dir="job_scripts",
        #                         db_info_file=None, account_name='scw1376', n_cores_per_job=5, n_jobs=2,
        #                         job_time_limit='2-23:59',
        #                         module_load='module load parallel singularity\n\n')

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

    create_tools = FindSen2ScnsGenDwnlds(cmd=script_cmd, sqlite_db_file="dwnld_sen2_scns.db")
    create_tools.parse_cmds()
