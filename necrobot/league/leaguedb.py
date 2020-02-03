"""
Interaction with the necrobot.leagues table.

Methods
-------
create_league(schema_name: str) -> None
    Make a new league.
get_league(schema_name: str) -> League
    Get a registered league.
get_entrant_ids() -> list[int]
    Get the NecroUser IDs of all entrants.
register_user(user_id: int) -> None
    Register a user in the `entrants` table.
write_league(League) -> None
    Write information in an already registered league to the database.

Exceptions
----------
LeagueAlreadyExists
LeagueDoesNotExist
InvalidSchemaName
"""

import re

import necrobot.exception
from necrobot.config import Config
from necrobot.database.dbconnect import DBConnect
from necrobot.database.dbwriter import DBWriter
from necrobot.league.league import League
from necrobot.match.matchinfo import MatchInfo
from necrobot.race.raceinfo import RaceInfo
from necrobot.race import racedb


class LeagueDBWriter(DBWriter):
    def __init__(self, schema_name=None):
        DBWriter.__init__(self, schema_name)

    async def create_league(self) -> League:
        """Creates a new CoNDOR event with the given schema_name as its database.

        Raises
        ------
        LeagueAlreadyExists
            When the schema_name already exists.
        """
        table_name_validator = re.compile(r'^[0-9a-zA-Z_$]+$')
        if not table_name_validator.match(self.schema_name):
            raise necrobot.exception.InvalidSchemaName()

        params = (self.schema_name,)
        async with DBConnect(commit=True) as cursor:
            cursor.execute(
                """
                SELECT `league_name` 
                FROM `leagues` 
                WHERE `schema_name`=%s
                """,
                params
            )
            for row in cursor:
                raise necrobot.exception.LeagueAlreadyExists(row[0])

            cursor.execute(
                """
                SELECT SCHEMA_NAME 
                FROM INFORMATION_SCHEMA.SCHEMATA 
                WHERE SCHEMA_NAME = %s
                """,
                params
            )
            for _ in cursor:
                raise necrobot.exception.LeagueAlreadyExists('Schema exists, but is not a CoNDOR event.')

            cursor.execute(
                """
                CREATE SCHEMA `{schema_name}` 
                DEFAULT CHARACTER SET = utf8 
                DEFAULT COLLATE = utf8_general_ci
                """.format(schema_name=self.schema_name)
            )
            cursor.execute(
                """
                INSERT INTO `leagues` 
                (`schema_name`) 
                VALUES (%s)
                """,
                params
            )

            cursor.execute(
                """
                CREATE TABLE `{schema_name}`.`entrants` (
                    `user_id` smallint unsigned NOT NULL,
                    PRIMARY KEY (`user_id`)
                ) DEFAULT CHARSET=utf8
                """.format(schema_name=self.schema_name)
            )

            for tablename in ['matches', 'match_races', 'races', 'race_runs', 'speedruns']:
                cursor.execute(
                    "CREATE TABLE `{league_schema}`.`{table}` LIKE `{necrobot_schema}`.`{table}`".format(
                        league_schema=self.schema_name,
                        necrobot_schema=Config.MYSQL_DB_NAME,
                        table=tablename
                    )
                )

            cursor.execute(
                """
                CREATE VIEW {race_summary} AS
                    SELECT 
                        {matches}.`match_id` AS `match_id`,
                        {match_races}.`race_number` AS `race_number`,
                        `users_winner`.`user_id` AS `winner_id`,
                        `users_loser`.`user_id` AS `loser_id`,
                        `race_runs_winner`.`time` AS `winner_time`,
                        `race_runs_loser`.`time` AS `loser_time`
                    FROM
                        {matches}
                        JOIN {match_races} ON {matches}.`match_id` = {match_races}.`match_id`
                        JOIN `users` `users_winner` ON 
                            IF( {match_races}.`winner` = 1, 
                                `users_winner`.`user_id` = {matches}.`racer_1_id`, 
                                `users_winner`.`user_id` = {matches}.`racer_2_id`
                            )
                        JOIN `users` `users_loser` ON 
                            IF( {match_races}.`winner` = 1, 
                                `users_loser`.`user_id` = {matches}.`racer_2_id`, 
                                `users_loser`.`user_id` = {matches}.`racer_1_id`
                            )
                        LEFT JOIN {race_runs} `race_runs_winner` ON 
                            `race_runs_winner`.`user_id` = `users_winner`.`user_id`
                            AND `race_runs_winner`.`race_id` = {match_races}.`race_id`
                        LEFT JOIN {race_runs} `race_runs_loser` ON 
                            `race_runs_loser`.`user_id` = `users_loser`.`user_id`
                            AND `race_runs_loser`.`race_id` = {match_races}.`race_id`
                    WHERE NOT {match_races}.`canceled`
                """.format(
                    matches=self.tn('matches'),
                    match_races=self.tn('match_races'),
                    race_runs=self.tn('race_runs'),
                    race_summary=self.tn('race_summary')
                )
            )

            cursor.execute(
                """
                CREATE VIEW {match_info} AS
                    SELECT 
                        {matches}.`match_id` AS `match_id`,
                        `ud1`.`twitch_name` AS `racer_1_name`,
                        `ud2`.`twitch_name` AS `racer_2_name`,
                        {matches}.`suggested_time` AS `scheduled_time`,
                        `ud3`.`twitch_name` AS `cawmentator_name`,
                        {matches}.`vod` AS `vod`,
                        {matches}.`is_best_of` AS `is_best_of`,
                        {matches}.`number_of_races` AS `number_of_races`,
                        {matches}.`autogenned` AS `autogenned`,
                        ({matches}.`r1_confirmed` AND {matches}.`r2_confirmed`) AS `scheduled`,
                        COUNT(0) AS `num_finished`,
                        SUM((CASE
                            WHEN ({match_races}.`winner` = 1) THEN 1
                            ELSE 0
                        END)) AS `racer_1_wins`,
                        SUM((CASE
                            WHEN ({match_races}.`winner` = 2) THEN 1
                            ELSE 0
                        END)) AS `racer_2_wins`,
                        (CASE
                            WHEN
                                {matches}.`is_best_of`
                            THEN
                                (GREATEST(SUM((CASE
                                            WHEN ({match_races}.`winner` = 1) THEN 1
                                            ELSE 0
                                        END)),
                                        SUM((CASE
                                            WHEN ({match_races}.`winner` = 2) THEN 1
                                            ELSE 0
                                        END))) >= (({matches}.`number_of_races` DIV 2) + 1))
                            ELSE (COUNT(0) >= {matches}.`number_of_races`)
                        END) AS `completed`
                    FROM
                        (((({matches}
                        LEFT JOIN {match_races} ON (({matches}.`match_id` = {match_races}.`match_id`)))
                        JOIN `necrobot`.`users` `ud1` ON (({matches}.`racer_1_id` = `ud1`.`user_id`)))
                        JOIN `necrobot`.`users` `ud2` ON (({matches}.`racer_2_id` = `ud2`.`user_id`)))
                        LEFT JOIN `necrobot`.`users` `ud3` ON (({matches}.`cawmentator_id` = `ud3`.`user_id`)))
                    WHERE
                        ({match_races}.`canceled` = 0 OR {match_races}.`canceled` IS NULL)
                    GROUP BY {matches}.`match_id`
                """.format(
                    match_info=self.tn('match_info'),
                    matches=self.tn('matches'),
                    match_races=self.tn('match_races')
                )
            )

            cursor.execute(
                """
                CREATE VIEW {league_info} AS
                    SELECT *
                    FROM `leagues`
                    WHERE (`leagues`.`schema_name` = %s)
                """.format(league_info=self.tn('league_info')),
                params
            )

        return League(
            commit_fn=self.write_league,
            schema_name=self.schema_name,
            league_name='<unnamed league>',
            match_info=MatchInfo()
        )

    async def get_entrant_ids(self) -> list:
        """Get NecroUser IDs for all entrants to the league

        Returns
        -------
        list[int]
        """
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT `user_id`
                FROM {entrants}
                """.format(entrants=self.tn('entrants'))
            )
            to_return = []
            for row in cursor:
                to_return.append(int(row[0]))
            return to_return

    async def get_league(self) -> League:
        """
        Returns
        -------
        League
            A League object for the event.
        """
        params = (self.schema_name,)
        async with DBConnect(commit=False) as cursor:
            cursor.execute(
                """
                SELECT 
                   `leagues`.`league_name`, 
                   `leagues`.`number_of_races`, 
                   `leagues`.`is_best_of`, 
                   `leagues`.`ranked`, 
                   `leagues`.`gsheet_id`,
                   `leagues`.`deadline`,
                   `race_types`.`character`, 
                   `race_types`.`descriptor`, 
                   `race_types`.`seeded`, 
                   `race_types`.`amplified`, 
                   `race_types`.`seed_fixed`,
                   `leagues`.`speedrun_gsheet_id` 
                FROM `leagues` 
                LEFT JOIN `race_types` ON `leagues`.`race_type` = `race_types`.`type_id` 
                WHERE `leagues`.`schema_name` = %s 
                LIMIT 1
                """,
                params
            )
            for row in cursor:
                race_info = RaceInfo()
                if row[6] is not None:
                    race_info.set_char(row[6])
                if row[7] is not None:
                    race_info.descriptor = row[7]
                if row[8] is not None:
                    race_info.seeded = bool(row[8])
                if row[9] is not None:
                    race_info.amplified = bool(row[9])
                if row[10] is not None:
                    race_info.seed_fixed = bool(row[10])

                match_info = MatchInfo(
                    max_races=int(row[1]) if row[1] is not None else None,
                    is_best_of=bool(row[2]) if row[2] is not None else None,
                    ranked=bool(row[3]) if row[3] is not None else None,
                    race_info=race_info
                )

                return League(
                    commit_fn=self.write_league,
                    schema_name=self.schema_name,
                    league_name=row[0],
                    match_info=match_info,
                    gsheet_id=row[4],
                    speedrun_gsheet_id=row[11],
                    deadline=row[5]
                )

            raise necrobot.exception.LeagueDoesNotExist()

    async def register_user(self, user_id: int) -> None:
        """Register the user for the league

        Parameters
        ----------
        user_id: int
            The user's NecroUser ID.
        """
        async with DBConnect(commit=True) as cursor:
            params = (user_id,)
            cursor.execute(
                """
                INSERT INTO {entrants}
                    (user_id)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE
                    user_id = VALUES(user_id)
                """.format(entrants=self.tn('entrants')),
                params
            )

    @staticmethod
    async def write_league(league: League) -> None:
        """Write the league to the database

        Parameters
        ----------
        league: League
            The league object to be written. Will create a new row if the schema_name is not in the database, and
            update otherwise.
        """
        match_info = league.match_info
        race_type_id = await racedb.get_race_type_id(race_info=match_info.race_info, register=True)

        async with DBConnect(commit=True) as cursor:
            params = (
                league.schema_name,
                league.name,
                league.gsheet_id,
                league.deadline,
                match_info.max_races,
                match_info.is_best_of,
                match_info.ranked,
                race_type_id,
                league.speedrun_gsheet_id
            )

            cursor.execute(
                """
                INSERT INTO `leagues` 
                (
                    `schema_name`, 
                    `league_name`, 
                    `gsheet_id`, 
                    `deadline`, 
                    `number_of_races`, 
                    `is_best_of`, 
                    `ranked`, 
                    `race_type`,
                    `speedrun_gsheet_id`
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON DUPLICATE KEY UPDATE
                   `league_name` = VALUES(`league_name`), 
                   `gsheet_id` = VALUES(`gsheet_id`),
                   `deadline` = VALUES(`deadline`),
                   `number_of_races` = VALUES(`number_of_races`), 
                   `is_best_of` = VALUES(`is_best_of`), 
                   `ranked` = VALUES(`ranked`), 
                   `race_type` = VALUES(`race_type`),
                   `speedrun_gsheet_id` = VALUES(`speedrun_gsheet_id`)
                """,
                params
            )
