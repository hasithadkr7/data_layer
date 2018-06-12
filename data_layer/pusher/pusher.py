from data_layer.timeseries import Timeseries
from data_layer.exceptions import NoTimeseriesFound


class Pusher:
    def __init__(self, session_creator):
        self.tms_operator = Timeseries(session_creator)

    def push_timeseries(self, meta_data, tms, force=False):
        tms_id = self.tms_operator.get_timeseries_id(meta_data)
        if tms_id is None:
            raise NoTimeseriesFound('No timeseries id found for the given meta data', meta_data)
        return self.tms_operator.update_timeseries(tms_id, tms, force)

    def push_collection(self, meta_dic):
        # TODO
        pass
