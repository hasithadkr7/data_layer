from data_layer.timeseries import Timeseries


class Extractor:
    def __init__(self, session_creator):
        self.tms_operator = Timeseries(session_creator)

    def pull_timeseries(self, meta_data, start_dt, end_dt):
        tms_id = self.tms_operator.get_timeseries_id(meta_data)
        return self.tms_operator.get_timeseries(tms_id, start_dt, end_dt)

    def pull_collection(self, meta_dic):
        output_dic = {}
        for key in meta_dic.keys():
            output_dic[key] = self.pull_timeseries(meta_dic[key], meta_dic[key]['from'], meta_dic[key]['to'])
        return output_dic
