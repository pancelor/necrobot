from necrobot.database import necrodb
from necrobot.race.race.raceinfo import RaceInfo
from necrobot.user.necrouser import NecroUser


def make_registered_match(*args, **kwargs):
    match = Match(*args, **kwargs)
    match.commit()
    return match


class Match(object):
    @staticmethod
    def get_from_id(match_id):
        raw_data = necrodb.get_raw_match_data(match_id)
        if raw_data is not None:
            return Match.make_from_raw_db_data(raw_data)

    @staticmethod
    def make_from_raw_db_data(row):
        race_info = necrodb.get_race_info_from_type_id(int(row[1])) if row[1] is not None else RaceInfo()
        cawmentator = NecroUser.get_user(user_id=int(row[11])) if row[11] is not None else None

        return Match(
            match_id=int(row[0]),
            race_info=race_info,
            racer_1_id=int(row[2]),
            racer_2_id=int(row[3]),
            suggested_time=row[4],
            r1_confirmed=bool(row[5]),
            r2_confirmed=bool(row[6]),
            r1_unconfirmed=bool(row[7]),
            r2_unconfirmed=bool(row[8]),
            is_best_of=bool(row[9]),
            max_races=int(row[10]),
            cawmentator=cawmentator
        )

    def __init__(self, racer_1_id, racer_2_id, max_races=3, is_best_of=False, match_id=None, suggested_time=None,
                 r1_confirmed=False, r2_confirmed=False, r1_unconfirmed=False, r2_unconfirmed=False,
                 race_info=RaceInfo(), cawmentator=None):
        self._match_id = match_id                   # int -- the unique ID for this match

        # Racers in the match
        self._racer_1_id = racer_1_id               # NecroUser
        self._racer_2_id = racer_2_id               # NecroUser

        # Scheduling data
        self._suggested_time = suggested_time       # datetime.datetime with pytz info attached
        self._confirmed_by_r1 = r1_confirmed
        self._confirmed_by_r2 = r2_confirmed
        self._r1_wishes_to_unconfirm = r1_unconfirmed
        self._r2_wishes_to_unconfirm = r2_unconfirmed

        # Format data
        self._number_of_races = max_races           # Maximum number of races
        self._is_best_of = is_best_of               # If true, end match after one player has clinched the most wins
        self._race_info = race_info                 # The kind of race the match will have

        # Viewer data
        self._cawmentator = cawmentator             # NecroUser

    def __eq__(self, other):
        return self.match_id == other.match_id

    @property
    def is_registered(self):
        return self._match_id is not None

    @property
    def match_id(self):
        return self._match_id

    @property
    def racers(self):
        return [self.racer_1, self.racer_2]

    @property
    def racer_1(self):
        return NecroUser.get_user(user_id=self._racer_1_id)

    @property
    def racer_2(self):
        return NecroUser.get_user(user_id=self._racer_2_id)

    @property
    def suggested_time(self):
        return self._suggested_time

    @property
    def confirmed_by_r1(self):
        return self._confirmed_by_r1

    @property
    def confirmed_by_r2(self):
        return self._confirmed_by_r2

    @property
    def r1_wishes_to_unconfirm(self):
        return self._r1_wishes_to_unconfirm

    @property
    def r2_wishes_to_unconfirm(self):
        return self._r2_wishes_to_unconfirm

    @property
    def has_suggested_time(self):
        return self.suggested_time is not None

    @property
    def is_scheduled(self):
        return self.confirmed_by_r1 and self.confirmed_by_r2

    @property
    def is_best_of(self):
        return self._is_best_of

    @property
    def number_of_races(self):
        return self._number_of_races

    @property
    def race_info(self):
        return self._race_info

    @property
    def cawmentator(self):
        return self._cawmentator

    @property
    def matchroom_name(self):
        name = ''
        for racer in self.racers:
            name += racer.discord_name + '-'
        return name[:-1] if name != '' else self.race_info.raceroom_name

    # Writes the match to the database
    def commit(self):
        necrodb.write_match(self)

    # Called by necrodb to set the match id. Do not call yourself.
    def set_match_id(self, match_id):
        self._match_id = match_id

    # Check whether the given user is in the match
    def racing_in_match(self, user):
        return user == self.racer_1 or user == self.racer_2

    # Suggest a time for the match
    def suggest_time(self, time):
        self.force_unconfirm()
        self._suggested_time = time

    # Whether the match has been confirmed by the racer
    def is_confirmed_by(self, racer):
        if racer.user_id == self._racer_1_id:
            return self._confirmed_by_r1
        elif racer.user_id == self._racer_2_id:
            return self._confirmed_by_r2
        else:
            return False

    # Confirm
    def confirm_time(self, racer):
        if racer.user_id == self._racer_1_id:
            self._confirmed_by_r1 = True
        elif racer.user_id == self._racer_2_id:
            self._confirmed_by_r2 = True

    # Unconfirm
    def unconfirm_time(self, racer):
        if racer.user_id == self._racer_1_id:
            if (not self._confirmed_by_r2) or self._r2_wishes_to_unconfirm:
                self.force_unconfirm()
            else:
                self._r1_wishes_to_unconfirm = True
        elif racer.user_id == self._racer_2_id:
            if (not self._confirmed_by_r1) or self._r1_wishes_to_unconfirm:
                self.force_unconfirm()
            else:
                self._r2_wishes_to_unconfirm = True

    # Force all to confirm
    def force_confirm(self):
        self._confirmed_by_r1 = True
        self._confirmed_by_r2 = True
        self._r1_wishes_to_unconfirm = False
        self._r2_wishes_to_unconfirm = False

    # Unconfirm (hard)
    def force_unconfirm(self):
        self._confirmed_by_r1 = False
        self._confirmed_by_r2 = False
        self._r1_wishes_to_unconfirm = False
        self._r2_wishes_to_unconfirm = False
        self._suggested_time = None

    # Set the match to be a repeat-N
    def set_repeat(self, number):
        self._is_best_of = False
        self._number_of_races = number

    # Set the match to be a best-of-N
    def set_best_of(self, number):
        self._is_best_of = True
        self._number_of_races = number