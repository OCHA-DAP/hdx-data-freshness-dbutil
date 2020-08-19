from datetime import datetime

from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from sqlalchemy import func


class DatabaseActions:
    def __init__(self, readsession, writesession, run_numbers):
        self.readsession = readsession
        self.writesession = writesession
        for i, run_number in enumerate(run_numbers):
            if run_number == 'latest':
                run_numbers[i] = readsession.query(func.max(DBRun.run_number)).scalar()
        self.run_numbers = run_numbers

    def copy_all(self, objects):
        for obj in objects:
            self.readsession.expunge(obj)
            self.writesession.merge(obj)
        self.writesession.commit()

    def duplicate(self):
        self.copy_all(self.readsession.query(DBRun))
        self.copy_all(self.readsession.query(DBOrganization))
        self.copy_all(self.readsession.query(DBInfoDataset))
        self.copy_all(self.readsession.query(DBDataset).filter(DBDataset.run_number.in_(self.run_numbers)))
        self.copy_all(self.readsession.query(DBResource).filter(DBResource.run_number.in_(self.run_numbers)))



