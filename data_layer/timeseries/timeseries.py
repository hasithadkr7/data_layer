import pandas as pd
import hashlib
import json

from datetime import datetime

from data_layer.models import Data, Run, RunView


class Timeseries:
    def __init__(self, session_creator):
        self.Session = session_creator

    @staticmethod
    def generate_timeseries_id(meta_data):
        """
        Generate the event id for given meta data.
        Only the 'station_name', 'variable', 'unit', 'event_type', 'source', 'run_name' are used
        to generate the id (i.e. hash value).
        An example meta data would look lik the following.
        {
            'station_name': 'Hanwella',
            'variable': 'Discharge',
            'unit': 'm3/s',
            'event_type': 'Forecast',
            'source': 'HEC-HMS',
            'run_name': 'Cloud Continuous'
        }
        :param meta_data: Dict with 'station_name', 'variable', 'unit', 'event_type', 'source', 'run_name' keys.
        :return: str: sha256 hash value in hex format (length of 64 characters).
        """
        sha256 = hashlib.sha256()
        hash_data = {
            'station_name': '',
            'variable': '',
            'unit': '',
            'event_type': '',
            'source': '',
            'run_name': ''
        }
        for key in hash_data.keys():
            hash_data[key] = meta_data[key]
        sha256.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        event_id = sha256.hexdigest()
        return event_id

    def get_timeseries_id(self, meta_data):
        """
        :param meta_data: Dict with 'station_name', 'variable', 'unit', 'event_type', 'source', 'run_name' keys.
        :return: timeseries id if exist else None.
        """
        session = self.Session()
        try:
            run_view_row = session.query(RunView).filter_by(name=meta_data['run_name'],
                                                            station=meta_data['station_name'],
                                                            variable=meta_data['variable'],
                                                            unit=meta_data['unit'],
                                                            type=meta_data['event_type'],
                                                            source=meta_data['source']
                                                            ).first()
            return None if run_view_row is None else run_view_row.id
        finally:
            session.close()

    def create_timeseries_id(self, run_name, station, variable, unit, event_type, source):
        tms_meta = {'station': station['name'],
                    'variable': variable['name'],
                    'unit': unit['name'],
                    'type': event_type['name'],
                    'source': source['name'],
                    'name': run_name}
        tms_id = Timeseries.generate_timeseries_id(tms_meta)
        run = Run(id=tms_id,
                  name=run_name,
                  station=station['id'],
                  variable=variable['id'],
                  unit=unit['id'],
                  type=event_type['id'],
                  source=source['id'])
        session = self.Session()
        try:
            session.add(run)
            session.commit()
            return tms_id
        finally:
            session.close()

    def get_timeseries(self, timeseries_id, start_date, end_date):
        """
        Retrieves the timeseries corresponding to given id s.t.
        time is in between given start_date (inclusive) and end_date (exclusive).

        :param timeseries_id: string timeseries id
        :param start_date: datetime object
        :param end_date: datetime object
        :return: array of [id, time, value]
        """

        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            raise ValueError('start_date and/or end_date are not of datetime type.', start_date, end_date)

        session = self.Session()
        try:
            result = session.query(Data).filter(
                Data.id == timeseries_id,
                Data.time >= start_date, Data.time < end_date
            ).all()
            timeseries = [[data_obj.time, data_obj.value] for data_obj in result]
            return pd.DataFrame(data=timeseries, columns=['time', 'value']).set_index(keys='time')
        finally:
            session.close()

    def get_timeseries_with_original_index(self, timeseries_id, start_date, end_date):
        """
        Retrieves the timeseries corresponding to given id s.t.
        time is in between given start_date (inclusive) and end_date (exclusive).

        :param timeseries_id: string timeseries id
        :param start_date: datetime object
        :param end_date: datetime object
        :return: array of [id, time, value]
        """

        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            raise ValueError('start_date and/or end_date are not of datetime type.', start_date, end_date)

        session = self.Session()
        try:
            result = session.query(Data).filter(
                Data.id == timeseries_id,
                Data.time >= start_date, Data.time < end_date
            ).all()
            timeseries = [[data_obj.time, data_obj.value] for data_obj in result]
            return pd.DataFrame(data=timeseries, columns=['time', 'value'])
        finally:
            session.close()

    def update_timeseries(self, timeseries_id, timeseries, should_overwrite):

        # timeseries should be a pnadas Dataframe, with 'time' as index, and 'value' as the column.
        if not isinstance(timeseries, pd.DataFrame):
            raise ValueError('The "timeseries" shoud be a pandas Dataframe containing (time, value) in a rows')

        session = self.Session()
        try:
            if should_overwrite:
                # update on conflict duplicate key.
                for index, row in timeseries.iterrows():
                    session.merge(Data(id=timeseries_id, time=index.to_pydatetime(), value=float(row['value'])))
                session.commit()
                return True

            else:
                # raise IntegrityError on duplicate key.
                data_obj_list = []
                for index, row in timeseries.iterrows():
                    data_obj_list.append(Data(id=timeseries_id, time=index.to_pydatetime(), value=float(row['value'])))

                session.bulk_save_objects(data_obj_list)
                session.commit()
                return True
        finally:
            session.close()
