import datetime
from typing import Optional, Dict

import necrobot.exception
from necrobot.league.leaguedb import LeagueDBWriter
from necrobot.league.league import League
from necrobot.botbase.manager import Manager
from necrobot.config import Config
from necrobot.util import console
from necrobot.util.parse import dateparse
from necrobot.util.singleton import Singleton
from necrobot.match.matchglobals import MatchGlobals


class LeagueMgr(Manager, metaclass=Singleton):
    _the_league = None          # type: Optional[League]
    _sub_leagues = dict()       # type: Dict[str, League]

    """Manager object for the global League, if any."""
    def __init__(self):
        pass

    @property
    def league(self) -> Optional[League]:
        return self._the_league

    def sub_league(self, league_name) -> Optional[League]:
        return self._sub_leagues[league_name] if league_name in self._sub_leagues else None

    def is_subleague(self, league_name) -> bool:
        return league_name in self._sub_leagues or league_name == self._the_league.schema_name

    async def initialize(self):
        if Config.LEAGUE_NAME:
            try:
                await self.set_league(schema_name=Config.LEAGUE_NAME, save_to_config=False)
            except necrobot.exception.LeagueDoesNotExist:
                console.warning(
                    'League "{0}" does not exist.'.format(Config.LEAGUE_NAME)
                )

    async def refresh(self):
        pass

    async def close(self):
        pass

    def on_botchannel_create(self, channel, bot_channel):
        pass

    @classmethod
    async def create_league(cls, schema_name: str, save_to_config=True):
        """Registers a new league
        
        Parameters
        ----------
        schema_name: str
            The schema name for the league
        save_to_config: bool
            Whether to make this the default league, i.e., save the schema name to the bot's config file
    
        Raises
        ------
        necrobot.database.leaguedb.LeagueAlreadyExists
            If the schema name refers to a registered league
        necrobot.database.leaguedb.InvalidSchemaName
            If the schema name is not a valid MySQL schema name
        """
        cls._the_league = await LeagueDBWriter(schema_name=schema_name).create_league()

        if save_to_config:
            Config.LEAGUE_NAME = schema_name
            Config.write()

    @classmethod
    async def set_league(cls, schema_name: str, save_to_config=True):
        """Set the current league
        
        Parameters
        ----------
        schema_name: str
            The schema name for the league
        save_to_config: bool
            Whether to make this the default league, i.e., save the schema name to the bot's config file
    
        Raises
        ------
        LeagueDoesNotExist
            If the schema name does not refer to a registered league
        """
        cls._the_league = await LeagueDBWriter(schema_name=schema_name).get_league()

        MatchGlobals().set_deadline_fn(LeagueMgr.deadline)

        if save_to_config:
            Config.LEAGUE_NAME = schema_name
            Config.write()

    async def create_sub_league(self, schema_name: str):
        """Registers a new sub-league for the current league

        Parameters
        ----------
        schema_name: str
            The schema name for the league

        Raises
        ------
        LeagueDoesNotExist
            If there is no set base league
        LeagueAlreadyExists
            If the schema name refers to a registered league
        InvalidSchemaName
            If the schema name is not a valid MySQL schema name
        """
        if self._the_league is None:
            raise necrobot.exception.LeagueDoesNotExist("Tried to create a sub-league of a NoneType league.")
        sub_league = await LeagueDBWriter(schema_name=schema_name).create_league()
        self._sub_leagues[schema_name] = sub_league
        # TODO: Set the sub-league's "parent" in the database and also locally to the base league

    @staticmethod
    def deadline() -> Optional[datetime.datetime]:
        if LeagueMgr._the_league is not None:
            deadline_str = LeagueMgr._the_league.deadline
            if deadline_str is not None:
                return dateparse.parse_datetime(deadline_str)
        return None
