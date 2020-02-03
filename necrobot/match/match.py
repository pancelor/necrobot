import datetime
from typing import Optional

import pytz

from botbase.necrobot import Necrobot
from database.dbconnect import DBConnect
from database.dbwriter import DBWriter
from match import matchdb
from match.matchgsheetinfo import MatchGSheetInfo
from match.matchinfo import MatchInfo
from match.matchracedata import MatchRaceData

from necrobot.match.matchgsheetinfo import MatchGSheetInfo
from necrobot.match.matchinfo import MatchInfo
from necrobot.race.raceinfo import RaceInfo
from necrobot.user import userlib
from necrobot.user.necrouser import NecroUser
from necrobot.util import console
from necrobot.util.decorators import commits
from necrobot.race.racedb import RaceDBWriter
from race import racedb
from race.raceinfo import RaceInfo
from util import server, console, timestr, rtmputil, strutil


class Match(object):
    def __init__(
            self,
            db_writer,
            racer_1_id,
            racer_2_id,
            match_id=None,
            suggested_time=None,
            r1_confirmed=False,
            r2_confirmed=False,
            r1_unconfirmed=False,
            r2_unconfirmed=False,
            match_info=MatchInfo(),
            cawmentator_id=None,
            channel_id=None,
            gsheet_info=None,
            finish_time=None,
            autogenned=False
    ):
        """Create a `Match` object. There should be no need to call this directly; use `matchutil.make_match` instead, 
        since this needs to interact with the database.
        
        Parameters
        ----------
        db_writer: MatchDBWriter
            Object for writing to the database.
        racer_1_id: int
            The DB user ID of the first racer.
        racer_2_id: int
            The DB user ID of the second racer.
        match_id: int
            The DB unique ID of this match.
        suggested_time: datetime.datetime
            The time the match is suggested for. If no tzinfo, UTC is assumed.
        r1_confirmed: bool
            Whether the first racer has confirmed the match time.
        r2_confirmed: bool
            Whether the second racer has confirmed the match time.
        r1_unconfirmed: bool
            Whether the first racer wishes to unconfirm the match time.
        r2_unconfirmed: bool
            Whether the second racer wishes to unconfirm the match time.
        match_info: MatchInfo
            The type of match.
        cawmentator_id: int
            The DB unique ID of the cawmentator for this match.
        channel_id: int
            The discord.ID of the channel for this match, if any.
        gsheet_info: MatchGSheetInfo
            If this match was created from a GSheet, the worksheet and row it was created from.
        finish_time: datetime.datetime
            The time the match finished at. If no tzinfo, UTC is assumed.
        """
        self._match_id = match_id

        # Racers in the match
        self._racer_1_id = racer_1_id                       # type: int
        self._racer_1 = None                                # type: Optional[NecroUser]
        self._racer_2_id = racer_2_id                       # type: int
        self._racer_2 = None                                # type: Optional[NecroUser]

        # Scheduling data
        self._suggested_time = None                         # type: Optional[datetime.datetime]
        self._finish_time = None                            # type: Optional[datetime.datetime]
        self._set_suggested_time(suggested_time)
        self._set_finish_time(finish_time)
        self._confirmed_by_r1 = r1_confirmed                # type: bool
        self._confirmed_by_r2 = r2_confirmed                # type: bool
        self._r1_wishes_to_unconfirm = r1_unconfirmed       # type: bool
        self._r2_wishes_to_unconfirm = r2_unconfirmed       # type: bool

        # Format and race data
        self._match_info = match_info                       # type: MatchInfo

        # Other
        self._cawmentator_id = int(cawmentator_id) if cawmentator_id is not None else None  # type: int
        self._channel_id = channel_id                       # type: int
        self._gsheet_info = gsheet_info                     # type: MatchGSheetInfo
        self._autogenned = autogenned                       # type: bool

        # Commit function
        self._db_writer = db_writer                         # type: MatchDBWriter

    def __repr__(self):
        return 'Match: <ID={mid}>, <ChannelName={cname}'.format(mid=self.match_id, cname=self.matchroom_name)

    def __eq__(self, other):
        return self.match_id == other.match_id

    def __str__(self):
        return self.matchroom_name

    async def initialize(self):
        self._racer_1 = await userlib.get_user(user_id=self._racer_1_id)
        self._racer_2 = await userlib.get_user(user_id=self._racer_2_id)
        if self._racer_1 is None or self._racer_2 is None:
            raise RuntimeError('Attempted to make a Match object with an unregistered racer.')

    @property
    def format_str(self) -> str:
        """Get a string describing the match format."""
        return self.match_info.format_str

    @property
    def ranked(self):
        return self._match_info.ranked

    @property
    def is_registered(self) -> bool:
        return self._match_id is not None

    @property
    def match_id(self) -> int:
        return self._match_id

    @property
    def racers(self) -> list:
        return [self.racer_1, self.racer_2]

    @property
    def racer_1(self) -> NecroUser:
        return self._racer_1

    @property
    def racer_2(self) -> NecroUser:
        return self._racer_2

    @property
    def suggested_time(self) -> datetime.datetime:
        return self._suggested_time

    @property
    def finish_time(self) -> datetime.datetime:
        return self._finish_time

    @property
    def confirmed_by_r1(self) -> bool:
        return self._confirmed_by_r1

    @property
    def confirmed_by_r2(self) -> bool:
        return self._confirmed_by_r2

    @property
    def r1_wishes_to_unconfirm(self) -> bool:
        return self._r1_wishes_to_unconfirm

    @property
    def r2_wishes_to_unconfirm(self) -> bool:
        return self._r2_wishes_to_unconfirm

    @property
    def has_suggested_time(self) -> bool:
        return self.suggested_time is not None

    @property
    def is_scheduled(self) -> bool:
        return self.has_suggested_time and self.confirmed_by_r1 and self.confirmed_by_r2

    @property
    def is_best_of(self) -> int:
        return self._match_info.is_best_of

    @property
    def number_of_races(self) -> int:
        return self._match_info.max_races

    @property
    def race_info(self) -> RaceInfo:
        return self._match_info.race_info

    @property
    def match_info(self) -> MatchInfo:
        return self._match_info

    @property
    def cawmentator_id(self) -> int:
        return self._cawmentator_id

    @property
    def channel_id(self) -> int:
        return self._channel_id

    @property
    def sheet_id(self) -> int:
        return self._gsheet_info.wks_id if self._gsheet_info is not None else None

    @property
    def sheet_row(self) -> int:
        return self._gsheet_info.row if self._gsheet_info is not None else None

    @property
    def autogenned(self) -> bool:
        return self._autogenned

    @property
    def matchroom_name(self) -> str:
        """Get a name for a channel for this match."""
        racer_names = []
        for racer in self.racers:
            racer_matchroom_name = racer.matchroom_name
            if racer_matchroom_name is not None:
                racer_names.append(racer_matchroom_name)

        if len(racer_names) == 2:
            racer_names.sort()
            return '{0}-{1}-{2}'.format(racer_names[0], racer_names[1], self.match_id)
        else:
            return self.race_info.raceroom_name

    @property
    def time_until_match(self) -> datetime.timedelta or None:
        return (self.suggested_time - pytz.utc.localize(datetime.datetime.utcnow())) if self.is_scheduled else None

    async def commit(self) -> None:
        """Write the match to the database."""
        await self._db_writer.write_match(self)

    async def get_cawmentator(self) -> NecroUser or None:
        if self._cawmentator_id is None:
            return None
        return await userlib.get_user(user_id=self._cawmentator_id)

    def racing_in_match(self, user: NecroUser) -> bool:
        """True if the user is in the match."""
        return user == self.racer_1 or user == self.racer_2

    def is_confirmed_by(self, racer: NecroUser) -> bool:
        """Whether the Match has been confirmed by racer."""
        if racer == self.racer_1:
            return self._confirmed_by_r1
        elif racer == self.racer_2:
            return self._confirmed_by_r2
        else:
            return False

    def set_match_id(self, match_id: int) -> None:
        """Sets the match ID. There should be no need to call this yourself."""
        self._match_id = match_id

    @commits
    def set_finish_time(self, time: datetime.datetime) -> None:
        """Sets the finishing time for the match. To the given time. If no tzinfo, UTC is assumed."""
        self._set_finish_time(time)

    @commits
    def suggest_time(self, time: datetime.datetime) -> None:
        """Unconfirms all previous times and suggests a new time for the match."""
        self.force_unconfirm()
        self._set_suggested_time(time)

    @commits
    def confirm_time(self, racer: NecroUser) -> None:
        """Confirms the current suggested time by the given racer. (The match is scheduled after
        both racers have confirmed.)"""
        if racer == self.racer_1:
            self._confirmed_by_r1 = True
        elif racer == self.racer_2:
            self._confirmed_by_r2 = True

    @commits
    def unconfirm_time(self, racer: NecroUser) -> None:
        """Attempts to unconfirm the current suggested time by the given racer. This deletes the 
        suggested time if either the match is not already scheduled or the other racer has also 
        indicated a desire to unconfirm."""
        if racer == self.racer_1:
            if (not self._confirmed_by_r2) or self._r2_wishes_to_unconfirm:
                self.force_unconfirm()
            else:
                self._r1_wishes_to_unconfirm = True
        elif racer == self.racer_2:
            if (not self._confirmed_by_r1) or self._r1_wishes_to_unconfirm:
                self.force_unconfirm()
            else:
                self._r2_wishes_to_unconfirm = True

    @commits
    def force_confirm(self) -> None:
        """Forces all racers to confirm the suggested time."""
        if self._suggested_time is None:
            console.warning('Tried to force_confirm a Match with no suggested time.')
            return
        self._confirmed_by_r1 = True
        self._confirmed_by_r2 = True
        self._r1_wishes_to_unconfirm = False
        self._r2_wishes_to_unconfirm = False

    @commits
    def force_unconfirm(self) -> None:
        """Unconfirms and deletes any current suggested time."""
        self._confirmed_by_r1 = False
        self._confirmed_by_r2 = False
        self._r1_wishes_to_unconfirm = False
        self._r2_wishes_to_unconfirm = False
        self._suggested_time = None

    @commits
    def set_repeat(self, number: int) -> None:
        """Sets the match type to be a repeat-X, where X is the input number."""
        self._match_info.is_best_of = False
        self._match_info.max_races = number

    @commits
    def set_best_of(self, number: int) -> None:
        """Sets the match type to be a best-of-X, where X is the input number."""
        self._match_info.is_best_of = True
        self._match_info.max_races = number

    @commits
    def set_race_info(self, race_info: RaceInfo) -> None:
        """Sets the type of races to be done in the match."""
        self._match_info.race_info = race_info

    @commits
    def set_cawmentator_id(self, cawmentator_id: Optional[int]) -> None:
        """Sets a cawmentator for the match. Using cawmentator_id = None will remove cawmentary."""
        self._cawmentator_id = cawmentator_id

    @commits
    def set_channel_id(self, channel_id: Optional[int]) -> None:
        """Sets a channel ID for the match."""
        self._channel_id = int(channel_id)

    @commits
    def raw_update(self, **kwargs):
        if 'suggested_time' in kwargs:
            self._set_suggested_time(kwargs['suggested_time'])
        if 'r1_confirmed' in kwargs:
            self._confirmed_by_r1 = kwargs['r1_confirmed']
        if 'r2_confirmed' in kwargs:
            self._confirmed_by_r2 = kwargs['r2_confirmed']
        if 'r1_unconfirmed' in kwargs:
            self._r1_wishes_to_unconfirm = kwargs['r1_unconfirmed']
        if 'r2_unconfirmed' in kwargs:
            self._r2_wishes_to_unconfirm = kwargs['r2_unconfirmed']
        if 'match_info' in kwargs:
            self._match_info = kwargs['match_info']
        if 'cawmentator_id' in kwargs:
            self._cawmentator_id = kwargs['cawmentator_id']
        if 'channel_id' in kwargs:
            self._channel_id = kwargs['channel_id']
        if 'gsheet_info' in kwargs:
            self._gsheet_info = kwargs['gsheet_info']
        if 'finish_time' in kwargs:
            self._finish_time = kwargs['finish_time']

    def _set_suggested_time(self, time: datetime.datetime or None) -> None:
        if time is None:
            self._suggested_time = None
            return
        if time.tzinfo is None:
            time = pytz.utc.localize(time)
        self._suggested_time = time.astimezone(pytz.utc)

    def _set_finish_time(self, time: datetime.datetime or None) -> None:
        if time is None:
            self._finish_time = None
            return
        if time.tzinfo is None:
            time = pytz.utc.localize(time)
        self._finish_time = time.astimezone(pytz.utc)


class MatchDBWriter(DBWriter):
    def __init__(self, schema_name):
        DBWriter.__init__(self, schema_name=schema_name)

    async def record_match_race(
            self,
            match: Match,
            race_number: int = None,
            race_id: int = None,
            winner: int = None,
            canceled: bool = False,
            contested: bool = False
            ) -> None:
        if race_number is None:
            race_number = await self._get_new_race_number(match)

        async with DBConnect(commit=True) as cursor:
            params = (
                match.match_id,
                race_number,
                race_id,
                winner,
                canceled,
                contested
            )

            cursor.execute(
                """
                INSERT INTO {match_races} 
                (match_id, race_number, race_id, winner, canceled, contested) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                ON DUPLICATE KEY UPDATE 
                   race_id=VALUES(race_id), 
                   winner=VALUES(winner), 
                   canceled=VALUES(canceled), 
                   contested=VALUES(contested)
                """.format(match_races=self.tn('match_races')),
                params
            )

    async def get_matches_between(self, user_1_id, user_2_id):
        params = (user_1_id, user_2_id, user_2_id, user_1_id,)

        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT
                    match_id
                FROM {matches}
                WHERE (racer_1_id = %s AND racer_2_id = %s) OR (racer_2_id = %s AND racer_1_id = %s)
                """.format(matches=self.tn('matches')),
                params
            )
            return cursor.fetchall()

    async def add_vod(self, match: Match, vodlink: str):
        if match.match_id is None:
            return

        params = (vodlink, match.match_id,)

        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE {matches}
                SET `vod`=%s
                WHERE `match_id`=%s
                """.format(matches=self.tn('matches')),
                params
            )

    async def set_match_race_contested(
            self,
            match: Match,
            race_number: int = None,
            contested: bool = True
            ) -> None:
        async with DBConnect(commit=True) as cursor:
            params = (
                contested,
                match.match_id,
                race_number,
            )

            cursor.execute(
                """
                UPDATE {match_races}
                SET `contested`=%s
                WHERE `match_id`=%s AND `race_number`=%s
                """.format(match_races=self.tn('match_races')),
                params
            )

    async def change_winner(self, match: Match, race_number: int, winner: int) -> bool:
        race_to_change = await self._get_uncanceled_race_number(match=match, race_number=race_number)
        if race_to_change is None:
            return False

        async with DBConnect(commit=True) as cursor:
            params = (
                winner,
                match.match_id,
                race_to_change,
            )

            cursor.execute(
                """
                UPDATE {match_races}
                SET `winner` = %s
                WHERE `match_id` = %s AND `race_number` = %s
                """.format(match_races=self.tn('match_races')),
                params
            )
            return True

    async def cancel_race(self, match: Match, race_number: int) -> bool:
        race_to_cancel = await self._get_uncanceled_race_number(match=match, race_number=race_number)
        if race_to_cancel is None:
            return False

        async with DBConnect(commit=True) as cursor:
            params = (
                match.match_id,
                race_to_cancel,
            )

            cursor.execute(
                """
                UPDATE {match_races}
                SET `canceled` = TRUE
                WHERE `match_id` = %s AND `race_number` = %s
                """.format(match_races=self.tn('match_races')),
                params
            )
            return True

    async def cancel_match(self, match: Match) -> bool:
        params = (match.match_id,)
        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                DELETE
                FROM {match_races}
                WHERE `match_id` = %s
                """.format(match_races=self.tn('match_races')),
                params
            )
            cursor.execute(
                """
                DELETE
                FROM {matches}
                WHERE `match_id` = %s
                """.format(matches=self.tn('matches')),
                params
            )
            return True

    async def write_match(self, match: Match):
        if not match.is_registered:
            await self._register_match(match)

        match_racetype_id = await RaceDBWriter.get_race_type_id(race_info=match.race_info, register=True)

        params = (
            match_racetype_id,
            match.racer_1.user_id,
            match.racer_2.user_id,
            match.suggested_time,
            match.confirmed_by_r1,
            match.confirmed_by_r2,
            match.r1_wishes_to_unconfirm,
            match.r2_wishes_to_unconfirm,
            match.ranked,
            match.is_best_of,
            match.number_of_races,
            match.cawmentator_id,
            match.channel_id,
            match.sheet_id,
            match.sheet_row,
            match.finish_time,
            match.autogenned,
            match.match_id,
        )

        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE {matches}
                SET
                   race_type_id=%s,
                   racer_1_id=%s,
                   racer_2_id=%s,
                   suggested_time=%s,
                   r1_confirmed=%s,
                   r2_confirmed=%s,
                   r1_unconfirmed=%s,
                   r2_unconfirmed=%s,
                   ranked=%s,
                   is_best_of=%s,
                   number_of_races=%s,
                   cawmentator_id=%s,
                   channel_id=%s,
                   sheet_id=%s,
                   sheet_row=%s,
                   finish_time=%s,
                   autogenned=%s
                WHERE match_id=%s
                """.format(matches=self.tn('matches')),
                params
            )

    async def register_match_channel(self, match_id: int, channel_id: int or None) -> None:
        params = (channel_id, match_id,)
        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE {matches}
                SET channel_id=%s
                WHERE match_id=%s
                """.format(matches=self.tn('matches')),
                params
            )

    async def get_match_channel_id(self, match_id: int) -> int:
        params = (match_id,)
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT channel_id 
                FROM {matches} 
                WHERE match_id=%s 
                LIMIT 1
                """.format(matches=self.tn('matches')),
                params
            )
            row = cursor.fetchone()
            return int(row[0]) if row[0] is not None else None

    async def get_channeled_matches_raw_data(
            self,
            must_be_scheduled: bool = False,
            order_by_time: bool = False,
            racer_id: int = None
    ) -> list:
        params = tuple()

        where_query = "`channel_id` IS NOT NULL"
        if must_be_scheduled:
            where_query += " AND (`suggested_time` IS NOT NULL AND `r1_confirmed` AND `r2_confirmed`)"
        if racer_id is not None:
            where_query += " AND (`racer_1_id` = %s OR `racer_2_id` = %s)"
            params += (racer_id, racer_id,)

        order_query = ''
        if order_by_time:
            order_query = "ORDER BY `suggested_time` ASC"

        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT 
                     match_id, 
                     race_type_id, 
                     racer_1_id, 
                     racer_2_id, 
                     suggested_time, 
                     r1_confirmed, 
                     r2_confirmed, 
                     r1_unconfirmed, 
                     r2_unconfirmed, 
                     ranked, 
                     is_best_of, 
                     number_of_races, 
                     cawmentator_id, 
                     channel_id,
                     sheet_id,
                     sheet_row,
                     finish_time,
                     autogenned
                FROM {matches} 
                WHERE {where_query} {order_query}
                """.format(matches=self.tn('matches'), where_query=where_query, order_query=order_query), params)
            return cursor.fetchall()

    async def get_matchview_raw_data(self):
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT 
                    match_id,
                    racer_1_name,
                    racer_2_name,
                    scheduled_time,
                    cawmentator_name,
                    racer_1_wins,
                    racer_2_wins,
                    completed,
                    vod,
                    autogenned,
                    scheduled
                FROM {match_info}
                ORDER BY -scheduled_time DESC
                """.format(match_info=self.tn('match_info'))
            )
            return cursor.fetchall()

    async def get_all_matches_raw_data(
            self,
            must_be_channeled: bool = False,
            must_be_scheduled: bool = False,
            order_by_time: bool = False,
            racer_id: int = None,
            limit: int = None
    ) -> list:
        params = tuple()

        where_query = 'TRUE'
        if must_be_channeled:
            where_query += " AND `channel_id` IS NOT NULL"
        if must_be_scheduled:
            where_query += " AND (`suggested_time` IS NOT NULL AND `r1_confirmed` AND `r2_confirmed`)"
        if racer_id is not None:
            where_query += " AND (`racer_1_id` = %s OR `racer_2_id` = %s)"
            params += (racer_id, racer_id,)

        order_query = ''
        if order_by_time:
            order_query = "ORDER BY `suggested_time` ASC"

        limit_query = '' if limit is None else 'LIMIT {}'.format(limit)

        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT 
                     match_id, 
                     race_type_id, 
                     racer_1_id, 
                     racer_2_id, 
                     suggested_time, 
                     r1_confirmed, 
                     r2_confirmed, 
                     r1_unconfirmed, 
                     r2_unconfirmed, 
                     ranked, 
                     is_best_of, 
                     number_of_races, 
                     cawmentator_id, 
                     channel_id,
                     sheet_id,
                     sheet_row,
                     finish_time,
                     autogenned
                FROM {matches} 
                WHERE {where_query} {order_query}
                {limit_query}
                """.format(
                    matches=self.tn('matches'),
                    where_query=where_query,
                    order_query=order_query,
                    limit_query=limit_query
                ), params)
            return cursor.fetchall()

    async def delete_match(self, match_id: int):
        params = (match_id,)
        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                DELETE FROM {match_races} 
                WHERE `match_id`=%s
                """.format(match_races=self.tn('match_races')),
                params
            )
            cursor.execute(
                """
                DELETE FROM {matches} 
                WHERE `match_id`=%s
                """.format(matches=self.tn('matches')),
                params
            )

    async def get_match_race_data(self, match_id: int) -> MatchRaceData:
        params = (match_id,)
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT canceled, winner 
                FROM {match_races} 
                WHERE match_id=%s
                """.format(match_races=self.tn('match_races')),
                params
            )
            finished = 0
            canceled = 0
            r1_wins = 0
            r2_wins = 0
            for row in cursor:
                if bool(row[0]):
                    canceled += 1
                else:
                    finished += 1
                    if int(row[1]) == 1:
                        r1_wins += 1
                    elif int(row[1]) == 2:
                        r2_wins += 1
            return MatchRaceData(finished=finished, canceled=canceled, r1_wins=r1_wins, r2_wins=r2_wins)

    async def get_match_id(
            self,
            racer_1_id: int,
            racer_2_id: int,
            scheduled_time: datetime.datetime = None,
            finished_only: Optional[bool] = None
    ) -> int or None:
        """Attempt to find a match between the two racers

        If multiple matches are found, prioritize as follows:
            1. Prefer matches closer to scheduled_time, if scheduled_time is not None
            2. Prefer channeled matches
            3. Prefer the most recent scheduled match
            4. Randomly

        Parameters
        ----------
        racer_1_id: int
            The user ID of the first racer
        racer_2_id: int
            The user ID of the second racer
        scheduled_time: datetime.datetime or None
            The approximate time to search around, or None to skip this priority
        finished_only: bool
            If not None, then: If True, only return matches that have a finish_time; if False, only return matches
            without

        Returns
        -------
        Optional[int]
            The match ID, if one is found.
        """
        param_dict = {
            'racer1': racer_1_id,
            'racer2': racer_2_id,
            'time': scheduled_time
        }

        where_str = '(racer_1_id=%(racer1)s AND racer_2_id=%(racer2)s) ' \
                    'OR (racer_1_id=%(racer2)s AND racer_2_id=%(racer1)s)'
        if finished_only is not None:
            where_str = '({old_str}) AND (finish_time IS {nullstate})'.format(
                old_str=where_str,
                nullstate=('NOT NULL' if finished_only else 'NULL')
            )

        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT 
                    match_id, 
                    suggested_time, 
                    channel_id,
                    ABS(`suggested_time` - '2017-23-04 12:00:00') AS abs_del
                FROM {matches}
                WHERE {where_str}
                ORDER BY
                    IF(%(time)s IS NULL, 0, -ABS(`suggested_time` - %(time)s)) DESC,
                    `channel_id` IS NULL ASC, 
                    `suggested_time` DESC
                LIMIT 1
                """.format(matches=self.tn('matches'), where_str=where_str),
                param_dict
            )
            row = cursor.fetchone()
            return int(row[0]) if row is not None else None

    async def get_fastest_wins_raw(self, limit: int = None) -> list:
        params = (limit,)
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT
                    {race_runs}.`time` AS `time`,
                    users_winner.twitch_name AS winner_name,
                    users_loser.twitch_name AS loser_name,
                    {matches}.suggested_time AS match_time
                FROM 
                    {match_races}
                    INNER JOIN {matches}
                        ON {matches}.match_id = {match_races}.match_id
                    INNER JOIN {races} 
                        ON {races}.race_id = {match_races}.race_id
                    INNER JOIN users users_winner 
                        ON IF(
                            {match_races}.winner = 1,
                            users_winner.`user_id` = {matches}.racer_1_id,
                            users_winner.`user_id` = {matches}.racer_2_id
                        )
                    INNER JOIN users users_loser 
                        ON IF(
                            {match_races}.winner = 1,
                            users_loser.user_id = {matches}.racer_2_id,
                            users_loser.user_id = {matches}.racer_1_id
                        )
                    INNER JOIN {race_runs}
                        ON ( 
                            {race_runs}.race_id = {races}.race_id
                            AND {race_runs}.user_id = users_winner.user_id
                        )
                WHERE
                    {match_races}.winner != 0
                ORDER BY `time` ASC
                LIMIT %s
                """.format(
                    race_runs=self.tn('race_runs'),
                    matches=self.tn('matches'),
                    match_races=self.tn('match_races'),
                    races=self.tn('races')
                ),
                params
            )
            return cursor.fetchall()

    async def get_matchstats_raw(self, user_id: int) -> list:
        params = (user_id,)
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS wins,
                    MIN(winner_time) AS best_win,
                    AVG(winner_time) AS average_win
                FROM {race_summary}
                WHERE winner_id = %s
                LIMIT 1
                """.format(race_summary=self.tn('race_summary')),
                params
            )
            winner_data = cursor.fetchone()
            if winner_data is None:
                winner_data = [0, None, None]
            cursor.execute(
                """
                SELECT COUNT(*) AS losses
                FROM {race_summary}
                WHERE loser_id = %s
                LIMIT 1
                """.format(race_summary=self.tn('race_summary')),
                params
            )
            loser_data = cursor.fetchone()
            if loser_data is None:
                loser_data = [0]
            return winner_data + loser_data

    async def get_raw_match_data(self, match_id: int) -> list:
        params = (match_id,)

        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT 
                     match_id, 
                     race_type_id, 
                     racer_1_id, 
                     racer_2_id, 
                     suggested_time, 
                     r1_confirmed, 
                     r2_confirmed, 
                     r1_unconfirmed, 
                     r2_unconfirmed, 
                     ranked, 
                     is_best_of, 
                     number_of_races, 
                     cawmentator_id, 
                     channel_id,
                     sheet_id,
                     sheet_row,
                     finish_time,
                     autogenned
                FROM {matches} 
                WHERE match_id=%s 
                LIMIT 1
                """.format(matches=self.tn('matches')),
                params
            )
            return cursor.fetchone()

    async def get_match_gsheet_duplication_number(self, match: Match) -> int:
        """
        Parameters
        ----------
        match: Match
            A Match registered in the database.

        Returns
        -------
        int
            If this Match was created from a GSheet, the number of matches on the same worksheet and with
            the same racers that appear in rows ahead of this match; otherwise, 0.
        """
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM {matches}
                WHERE
                    (racer_1_id = %(r1id)s OR racer_1_id = %(r2id)s)
                    AND (racer_2_id = %(r1id)s OR racer_2_id = %(r2id)s)
                    AND sheet_id = %(sheetid)s
                    AND sheet_row < %(sheetrow)s
                """.format(matches=self.tn('matches')),
                {
                    'r1id': match.racer_1.user_id,
                    'r2id': match.racer_2.user_id,
                    'sheetid': match.sheet_id,
                    'sheetrow': match.sheet_row,
                }
            )
            return int(cursor.fetchone()[0])

    async def scrub_unchanneled_unraced_matches(self) -> None:
        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                DELETE {matches}
                FROM {matches}
                LEFT JOIN (
                    SELECT 
                        match_id, 
                        COUNT(*) AS number_of_races 
                    FROM {match_races}
                    GROUP BY match_id
                ) match_counts ON match_counts.match_id = {matches}.match_id
                WHERE match_counts.number_of_races IS NULL AND {matches}.channel_id IS NULL
                """.format(
                    matches=self.tn('matches'),
                    match_races=self.tn('match_races')
                )
            )

    async def _register_match(self, match: Match) -> None:
        match_racetype_id = await RaceDBWriter.get_race_type_id(race_info=match.race_info, register=True)

        params = (
            match_racetype_id,
            match.racer_1.user_id,
            match.racer_2.user_id,
            match.suggested_time,
            match.confirmed_by_r1,
            match.confirmed_by_r2,
            match.r1_wishes_to_unconfirm,
            match.r2_wishes_to_unconfirm,
            match.ranked,
            match.is_best_of,
            match.number_of_races,
            match.cawmentator_id,
            match.finish_time,
            match.autogenned
        )

        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                INSERT INTO {matches} 
                (
                   race_type_id, 
                   racer_1_id, 
                   racer_2_id, 
                   suggested_time, 
                   r1_confirmed, 
                   r2_confirmed, 
                   r1_unconfirmed, 
                   r2_unconfirmed, 
                   ranked, 
                   is_best_of, 
                   number_of_races, 
                   cawmentator_id,
                   finish_time,
                   autogenned
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(matches=self.tn('matches')),
                params
            )
            cursor.execute("SELECT LAST_INSERT_ID()")
            match.set_match_id(int(cursor.fetchone()[0]))

            params = (match.racer_1.user_id, match.racer_2.user_id,)
            cursor.execute(
                """
                INSERT IGNORE INTO {entrants} (user_id)
                VALUES (%s), (%s)
                """.format(entrants=self.tn('entrants')),
                params
            )

    async def _get_uncanceled_race_number(self, match: Match, race_number: int) -> int or None:
        params = (match.match_id,)
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT `race_number` 
                FROM {0} 
                WHERE `match_id` = %s AND `canceled` = FALSE 
                ORDER BY `race_number` ASC
                """.format(self.tn('match_races')),
                params
            )
            races = cursor.fetchall()
            if len(races) < race_number:
                return None

            return int(races[race_number - 1][0])

    async def _get_new_race_number(self, match: Match) -> int:
        params = (match.match_id,)
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT `race_number` 
                FROM {0} 
                WHERE `match_id` = %s 
                ORDER BY `race_number` DESC 
                LIMIT 1
                """.format(self.tn('match_races')),
                params
            )
            row = cursor.fetchone()
            return int(row[0]) + 1 if row is not None else 1


__match_library = {}


def invalidate_cache():
    global __match_library
    __match_library = {}


async def make_match(*args, register=False, update=False, **kwargs) -> Optional[Match]:
    """Create a Match object.

    Parameters
    ----------
    racer_1_id: int
        The DB user ID of the first racer.
    racer_2_id: int
        The DB user ID of the second racer.
    max_races: int
        The maximum number of races this match can be. (If is_best_of is True, then the match is a best of
        max_races; otherwise, the match is just repeating max_races.)
    match_id: int
        The DB unique ID of this match. If this parameter is specified, the return value may be None, if no match
        in the database has the specified ID.
    suggested_time: datetime.datetime
        The time the match is suggested for. If no tzinfo, UTC is assumed.
    r1_confirmed: bool
        Whether the first racer has confirmed the match time.
    r2_confirmed: bool
        Whether the second racer has confirmed the match time.
    r1_unconfirmed: bool
        Whether the first racer wishes to unconfirm the match time.
    r2_unconfirmed: bool
        Whether the second racer wishes to unconfirm the match time.
    match_info: MatchInfo
        The types of races to be run in this match.
    cawmentator_id: int
        The DB unique ID of the cawmentator for this match.
    sheet_id: int
        The sheetID of the worksheet the match was created from, if any.
    register: bool
        Whether to register the match in the database.
    update: bool
        If match_id is given and this is True, updates the database match with any other specified parameters.

    Returns
    ---------
    Match
        The created match.
    """
    global __match_library

    if 'match_id' in kwargs and kwargs['match_id'] is not None:
        cached_match = await get_match_from_id(kwargs['match_id'])
        if update and cached_match is not None:
            cached_match.raw_update(**kwargs)
            await cached_match.commit()
        return cached_match

    match = Match(*args, db_writer=MatchDBWriter(schema_name=None), **kwargs)
    await match.initialize()
    if register:
        await match.commit()
        __match_library[match.match_id] = match
    return match


async def get_match_from_id(match_id: int) -> Match or None:
    """Get a match object from its DB unique ID.

    Parameters
    ----------
    match_id: int
        The databse ID of the match.

    Returns
    -------
    Optional[Match]
        The match found, if any.
    """
    global __match_library

    if match_id is None:
        return None

    if match_id in __match_library:
        return __match_library[match_id]

    raw_data = await matchdb.get_raw_match_data(match_id)
    if raw_data is not None:
        return await make_match_from_raw_db_data(raw_data)
    else:
        return None


async def get_upcoming_and_current() -> list:
    """
    Returns
    -------
    list[Match]
        A list of all upcoming and ongoing matches, in order.
    """
    matches = []
    for row in await matchdb.get_channeled_matches_raw_data(must_be_scheduled=True, order_by_time=True):
        channel_id = int(row[13]) if row[13] is not None else None
        if channel_id is not None:
            channel = server.find_channel(channel_id=channel_id)
            if channel is not None:
                match = await make_match_from_raw_db_data(row=row)
                if match.suggested_time is None:
                    console.warning('Found match object {} has no suggested time.'.format(repr(match)))
                    continue
                if match.suggested_time > pytz.utc.localize(datetime.datetime.utcnow()):
                    matches.append(match)
                else:
                    match_room = Necrobot().get_bot_channel(channel)
                    if match_room is not None and await match_room.during_races():
                        matches.append(match)

    return matches


async def get_nextrace_displaytext(match_list: list) -> str:
    utcnow = pytz.utc.localize(datetime.datetime.utcnow())
    if len(match_list) > 1:
        display_text = 'Upcoming matches: \n'
    else:
        display_text = 'Next match: \n'

    for match in match_list:
        # noinspection PyUnresolvedReferences
        display_text += '\N{BULLET} **{0}** - **{1}**'.format(
            match.racer_1.display_name,
            match.racer_2.display_name)
        if match.suggested_time is None:
            display_text += '\n'
            continue

        display_text += ': {0} \n'.format(timestr.timedelta_to_str(match.suggested_time - utcnow, punctuate=True))
        match_cawmentator = await match.get_cawmentator()
        if match_cawmentator is not None:
            display_text += '    Cawmentary: <http://www.twitch.tv/{0}> \n'.format(match_cawmentator.twitch_name)
        elif match.racer_1.twitch_name is not None and match.racer_2.twitch_name is not None:
            display_text += '    Kadgar: {} \n'.format(
                rtmputil.kadgar_link(match.racer_1.twitch_name, match.racer_2.twitch_name)
            )

    display_text += '\nFull schedule: <https://condor.host/schedule>'

    return display_text


async def delete_match(match_id: int) -> None:
    global __match_library

    await matchdb.delete_match(match_id=match_id)
    if match_id in __match_library:
        del __match_library[match_id]


async def make_match_from_raw_db_data(row: list) -> Match:
    global __match_library

    match_id = int(row[0])
    if match_id in __match_library:
        return __match_library[match_id]

    match_info = MatchInfo(
        race_info=await racedb.get_race_info_from_type_id(int(row[1])) if row[1] is not None else RaceInfo(),
        ranked=bool(row[9]),
        is_best_of=bool(row[10]),
        max_races=int(row[11])
    )

    sheet_info = MatchGSheetInfo()
    sheet_info.wks_id = row[14]
    sheet_info.row = row[15]

    new_match = Match(
        commit_fn=matchdb.write_match,
        match_id=match_id,
        match_info=match_info,
        racer_1_id=int(row[2]),
        racer_2_id=int(row[3]),
        suggested_time=row[4],
        finish_time=row[16],
        r1_confirmed=bool(row[5]),
        r2_confirmed=bool(row[6]),
        r1_unconfirmed=bool(row[7]),
        r2_unconfirmed=bool(row[8]),
        cawmentator_id=row[12],
        channel_id=int(row[13]) if row[13] is not None else None,
        gsheet_info=sheet_info,
        autogenned=bool(row[17])
    )

    await new_match.initialize()
    __match_library[new_match.match_id] = new_match
    return new_match


async def get_schedule_infotext():
    utcnow = pytz.utc.localize(datetime.datetime.utcnow())
    matches = await get_upcoming_and_current()

    max_r1_len = 0
    max_r2_len = 0
    for match in matches:
        max_r1_len = max(max_r1_len, len(strutil.tickless(match.racer_1.display_name)))
        max_r2_len = max(max_r2_len, len(strutil.tickless(match.racer_2.display_name)))

    schedule_text = '``` \nUpcoming matches: \n'
    for match in matches:
        if len(schedule_text) > 1800:
            break
        schedule_text += '{r1:>{w1}} v {r2:<{w2}} : '.format(
            r1=strutil.tickless(match.racer_1.display_name),
            w1=max_r1_len,
            r2=strutil.tickless(match.racer_2.display_name),
            w2=max_r2_len
        )
        if match.suggested_time - utcnow < datetime.timedelta(minutes=0):
            schedule_text += 'Right now!'
        else:
            schedule_text += timestr.str_full_24h(match.suggested_time)
        schedule_text += '\n'
    schedule_text += '```'

    return schedule_text


async def get_race_data(match: Match):
    return await matchdb.get_match_race_data(match.match_id)


async def match_exists_between(racer_1, racer_2) -> bool:
    prior_match_ids = await matchdb.get_matches_between(racer_1.user_id, racer_2.user_id)
    return bool(prior_match_ids)