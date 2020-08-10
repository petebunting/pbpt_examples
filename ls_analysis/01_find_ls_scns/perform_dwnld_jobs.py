from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import subprocess
import os
from arcsilib.arcsiscnprocessdb import RecordScn2Process

logger = logging.getLogger(__name__)

class PerformScnDownload(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='perform_dwnld_jobs.py', descript=None)

    def do_processing(self, **kwargs):
        scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
        downloaded = scn_rcd_obj.is_scn_downloaded(self.params['product_id'], self.params['sensor'])
        if not downloaded:
            cmd = "gsutil -m cp -r {} {}".format(self.params['scn_url'], self.params['downpath'])
            logger.debug("Running command: '{}'".format(cmd))
            subprocess.call(cmd, shell=True)
            scn_rcd_obj.set_scn_downloaded(self.params['product_id'], self.params['sensor'], self.params['downpath'])

    def required_fields(self, **kwargs):
        return ["product_id", "sensor", "scn_url", "downpath", "scn_db_file", "goog_key_json"]

    def outputs_present(self, **kwargs):
        scn_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        downloaded = scn_rcd_obj.is_scn_downloaded(self.params['product_id'], self.params['sensor'])
        return downloaded, dict()

    def remove_outputs(self, **kwargs):
        # Remove the output files.
        scn_rcd_obj = RecordSen2Process(self.params['scn_db_file'])
        scn_rcd_obj.reset_all_scn(self.params['product_id'], self.params['sensor'], delpath=True)
        if not os.path.exists(self.params['downpath']):
            os.mkdir(self.params['downpath'])

if __name__ == "__main__":
    PerformScnDownload().std_run()


