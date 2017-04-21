from necrobot.database.dbconnect import DBConnect


def record_race(race):
    with DBConnect(commit=True) as cursor:
        # Find the race type
        racetype_params = (race.race_info.character_str,
                           race.race_info.descriptor,
                           race.race_info.seeded,
                           race.race_info.amplified,
                           race.race_info.seed_fixed)
        cursor.execute(
            "SELECT type_id "
            "FROM race_types "
            "WHERE `character`=%s "
            "   AND descriptor=%s "
            "   AND seeded = %s "
            "   AND amplified = %s "
            "   AND seed_fixed = %s",
            racetype_params)

        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                "INSERT INTO race_types "
                "(`character`, descriptor, seeded, amplified, seed_fixed) "
                "VALUES (%s, %s, %s, %s, %s)",
                racetype_params)
            cursor.execute("SELECT LAST_INSERT_ID()")

        type_id = int(row[0])

        # Record the race
        race_params = (
            race.start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            type_id,
            race.race_info.seed,
            race.race_info.condor_race,
            race.race_info.private_race,
        )

        cursor.execute(
            "INSERT INTO race_data "
            "(timestamp, type_id, seed, condor, private) "
            "VALUES (%s,%s,%s,%s,%s)",
            race_params)

        # Store the new race ID in the Race object
        cursor.execute("SELECT LAST_INSERT_ID()")
        race.race_id = int(cursor.fetchone()[0])

        # Record each racer in racer_data
        rank = 1
        for racer in race.racers:
            racer_params = (race.race_id, racer.id, racer.time, rank, racer.igt, racer.comment, racer.level)
            cursor.execute(
                "INSERT INTO racer_data "
                "(race_id, discord_id, time, rank, igt, comment, level) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                racer_params)
            if racer.is_finished:
                rank += 1

            # Update the user's name in the database
            user_params = (racer.id, racer.name)
            cursor.execute(
                'INSERT INTO user_data '
                '(discord_id, discord_name) '
                'VALUES (%s,%s) '
                'ON DUPLICATE KEY UPDATE '
                'discord_name=VALUES(discord_name)',
                user_params)


def get_allzones_race_numbers(discord_id, amplified):
    with DBConnect(commit=False) as cursor:
        params = (discord_id,)
        cursor.execute(
            "SELECT race_types.character, COUNT(*) as num "
            "FROM racer_data "
            "INNER JOIN race_data ON race_data.race_id = racer_data.race_id "
            "INNER JOIN race_types ON race_data.type_id = race_types.type_id "
            "WHERE racer_data.discord_id = %s "
            "AND race_types.descriptor = 'All-zones' " +
            ("AND race_types.amplified " if amplified else "AND NOT race_types.amplified ") +
            "AND race_types.seeded AND NOT race_data.private "
            "GROUP BY race_types.character "
            "ORDER BY num DESC",
            params)
        return cursor.fetchall()


def get_all_racedata(discord_id, char_name, amplified):
    with DBConnect(commit=False) as cursor:
        params = (discord_id, char_name)
        cursor.execute(
            "SELECT racer_data.time, racer_data.level "
            "FROM racer_data "
            "INNER JOIN race_data ON race_data.race_id = racer_data.race_id "
            "INNER JOIN race_types ON race_data.type_id = race_types.type_id "
            "WHERE racer_data.discord_id = %s "
            "AND race_types.character = %s "
            "AND race_types.descriptor = 'All-zones' " +
            ("AND race_types.amplified " if amplified else "AND NOT race_types.amplified ") +
            "AND race_types.seeded AND NOT race_data.private ",
            params)
        return cursor.fetchall()


def get_fastest_times_leaderboard(character_name, amplified, limit):
    with DBConnect(commit=False) as cursor:
        params = (character_name, limit,)
        cursor.execute(
            "SELECT user_data.discord_name, racer_data.time, race_data.seed, race_data.timestamp "
            "FROM racer_data "
            "INNER JOIN "
            "( "
            "    SELECT discord_id, MIN(time) AS min_time "
            "    FROM racer_data "
            "    INNER JOIN race_data ON race_data.race_id = racer_data.race_id "
            "    INNER JOIN race_types ON race_types.type_id = race_data.type_id "
            "    WHERE "
            "        racer_data.time > 0 "
            "        AND racer_data.level = -2 "
            "        AND race_types.character=%s "
            "        AND race_types.descriptor='All-zones' "
            "        AND race_types.seeded " +
            "        AND {0}race_types.amplified ".format('' if amplified else 'NOT ') +
            "        AND NOT race_data.private "
            "    GROUP BY discord_id "
            ") rd1 On rd1.discord_id = racer_data.discord_id "
            "INNER JOIN user_data ON user_data.discord_id = racer_data.discord_id "
            "INNER JOIN race_data ON race_data.race_id = racer_data.race_id "
            "WHERE racer_data.time = rd1.min_time "
            "ORDER BY racer_data.time ASC "
            "LIMIT %s",
            params)
        return cursor.fetchall()


def get_most_races_leaderboard(character_name, limit):
    with DBConnect(commit=False) as cursor:
        params = (character_name, character_name, limit,)
        cursor.execute(
            "SELECT "
            "    user_name, "
            "    num_predlc + num_postdlc as total, "
            "    num_predlc, "
            "    num_postdlc "
            "FROM "
            "( "
            "    SELECT "
            "        user_data.discord_name as user_name, "
            "        SUM( "
            "                IF( "
            "                   race_types.character=%s "
            "                   AND race_types.descriptor='All-zones' "
            "                   AND NOT race_types.amplified "
            "                   AND NOT race_data.private, "
            "                   1, 0 "
            "                ) "
            "        ) as num_predlc, "
            "        SUM( "
            "                IF( "
            "                   race_types.character=%s "
            "                   AND race_types.descriptor='All-zones' "
            "                   AND race_types.amplified "
            "                   AND NOT race_data.private, "
            "                   1, 0 "
            "                ) "
            "        ) as num_postdlc "
            "    FROM racer_data "
            "    INNER JOIN user_data ON user_data.discord_id = racer_data.discord_id "
            "    INNER JOIN race_data ON race_data.race_id = racer_data.race_id "
            "    INNER JOIN race_types ON race_types.type_id = race_data.type_id "
            "    GROUP BY user_data.discord_name "
            ") tbl1 "
            "ORDER BY total DESC "
            "LIMIT %s",
            params)
        return cursor.fetchall()


def get_race_type_id(race_info, register=False):
    params = (
        race_info.character_str,
        race_info.descriptor,
        race_info.seeded,
        race_info.amplified,
        race_info.seed_fixed,
    )

    with DBConnect(commit=False) as cursor:
        cursor.execute(
            "SELECT `type_id` "
            "FROM `race_types` "
            "WHERE `character`=%s "
            "   AND `descriptor`=%s "
            "   AND `seeded`=%s "
            "   AND `amplified`=%s "
            "   AND `seed_fixed`=%s "
            "LIMIT 1",
            params
        )

        row = cursor.fetchone()
        if row is not None:
            return int(row[0])

    # If here, the race type was not found
    if not register:
        return None

    # Create the new race type
    with DBConnect(commit=True) as cursor:
        cursor.execute(
            "INSERT INTO race_types "
            "(`character`, descriptor, seeded, amplified, seed_fixed) "
            "VALUES (%s, %s, %s, %s, %s)",
            params
        )
        cursor.execute("SELECT LAST_INSERT_ID()")
        return int(cursor.fetchone()[0])