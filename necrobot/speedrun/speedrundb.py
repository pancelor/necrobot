"""
Module for interacting with the speedruns table of the database.
"""

import datetime

from necrobot.database.dbconnect import DBConnect
from necrobot.database.dbwriter import DBWriter
from necrobot.race.racedb import RaceDBWriter
from necrobot.race.raceinfo import RaceInfo
from necrobot.user.necrouser import NecroUser


class SpeedrunDBWriter(DBWriter):
    def __init__(self, schema_name):
        DBWriter.__init__(self, schema_name=schema_name)

    async def submit(
            self,
            necro_user: NecroUser,
            category_race_info: RaceInfo,
            category_score: int,
            vod_url: str,
            submission_time: datetime.datetime = None
    ) -> None:
        category_type_id = await RaceDBWriter.get_race_type_id(race_info=category_race_info, register=True)

        params = (
            necro_user.user_id,
            category_type_id,
            category_score,
            vod_url,
            submission_time
        )
        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                INSERT INTO {speedruns}
                (user_id, type_id, score, vod, submission_time)
                VALUES (%s, %s, %s, %s, %s)
                """.format(speedruns=self.tn('speedruns')),
                params
            )

    async def get_raw_data(self):
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT 
                    submission_id,
                    user_id,
                    type_id,
                    score,
                    vod,
                    submission_time,
                    verified
                FROM {speedruns}
                ORDER BY -submission_time DESC
                """.format(speedruns=self.tn('speedruns'))
            )
            return cursor.fetchall()

    async def set_verified(self, run_id: int, verified: bool):
        async with DBConnect(commit=True) as cursor:
            params = (verified, run_id,)
            cursor.execute(
                """
                UPDATE {speedruns}
                SET verified = %s
                WHERE submission_id = %s
                """.format(speedruns=self.tn('speedruns')),
                params
            )
