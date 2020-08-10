from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import arcsilib
import arcsilib.arcsirun
import os
import shutil
from arcsilib.arcsiscnprocessdb import RecordScn2Process

logger = logging.getLogger(__name__)

class PerformScnARD(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='perform_ard_process.py', descript=None)

    def do_processing(self, **kwargs):
        scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
        downloaded = scn_rcd_obj.is_scn_downloaded(self.params['product_id'], self.params['sensor'])
        ard_processed = scn_rcd_obj.is_scn_ard(self.params['product_id'], self.params['sensor'])
        if downloaded and (not ard_processed):
            input_hdr = self.find_first_file(self.params['dwnld_path'], "*MTL.txt")

            arcsilib.arcsirun.runARCSI(input_hdr, None, None, self.params['sensor'], None, "KEA",
                                       self.params['ard_path'], None, None, None, None, None, None,
                                       ["CLOUDS", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW",
                                        "METADATA"],
                                       True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH,
                                       arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                                       "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                                       None, None, self.params['tmp_dir'], 0.05, 0.5, 0.1, 0.4, self.params['dem'],
                                       None, None, True, 20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                                       True, False, False, None, None, True, None, 'LSMSK')
            scn_rcd_obj.set_scn_ard(self.params['product_id'], self.params['sensor'], self.params['ard_path'])
        elif ard_processed:
            scn_rcd_obj.set_scn_ard(self.params['product_id'], self.params['sensor'], self.params['ard_path'])

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["product_id", "sensor", "dwnld_path", "ard_path", "scn_db_file", "tmp_dir", "dem"]

    def outputs_present(self, **kwargs):
        scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
        ard_processed = scn_rcd_obj.is_scn_ard(self.params['product_id'], self.params['sensor'])
        return ard_processed, dict()

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output files.
        scn_rcd_obj = RecordScn2Process(self.params['scn_db_file'])
        scn_rcd_obj.reset_ard_scn(self.params['product_id'], self.params['sensor'], delpath=True)
        if not os.path.exists(self.params['ard_path']):
            os.mkdir(self.params['ard_path'])


if __name__ == "__main__":
    PerformScnARD().std_run()


