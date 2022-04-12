import aiohttp
from datetime import datetime as dt
import datetime
import discord #upm package(py-cord)
from discord.ext import tasks #upm package(py-cord)
import json
from keep_alive import keep_alive
import os
import pandas as pd
import re

from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
BOT_ADMINS = [int(x.strip()) for x in os.getenv('BOT_ADMINS').replace('[', '').replace(']', '').split(',')]
TEXT_CHANNEL_IDS = [int(x.strip()) for x in
                    os.getenv('TEXT_CHANNEL_IDS').replace('[', '').replace(']', '').split(',')]
VOICE_CHANNEL_IDS = [int(x.strip()) for x in os.getenv('VOICE_CHANNEL_IDS').replace('[', '').replace(']', '').split(',')]
intents = discord.Intents.default()
bot = discord.Bot(intents=intents)
# bot.intents.scheduled_events = True

with open("res/f1_dict.json", "r") as f:
    f1_dict = json.load(f)


@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')


@bot.event
async def on_message(message):
    if not message.guild and message.author.id in BOT_ADMINS and message.content.lower().startswith('session'):
        msg = message.content.lower().split()
        if len(msg) < 2:
            return
        current, df = await fetch_calendar()
        end = False if "start" in msg else True
        for x in msg:
            minutes = re.match("-?\d{1,3}", x)
            if minutes:
                minutes = int(minutes.group())
                break
        if not minutes:
            return
        await session_adjust(current, df, minutes, message, end)

    elif message.author.id != bot.user.id and message.channel.id in TEXT_CHANNEL_IDS:
        await message.delete()
    return


# MAIN FUNCTIONS

async def fetch_calendar(
    path: str = "res/calendars",
    year: int = int(dt.now(datetime.timezone.utc).year),
):

    current = await get_current()
    if not os.path.exists(f"{path}/{year}"):
        os.makedirs(f"{path}/{year}")
    files = os.listdir(f"{path}/{year}")
    latest_csv = f"{path}/{year}/{(sorted(files))[-1]}" if files else None
    if not latest_csv:
        from session_times import generate_current_calendar
        generate_current_calendar()
        latest_csv = f"{path}/{year}/{(sorted(files))[-1]}" if files else None
        if not latest_csv:
            return current, None

    current["latest_csv"] = latest_csv
    df = pd.read_csv(latest_csv)
    return current, df


async def update_current_params(current, df):
    if df is not None:
        now = int(dt.timestamp(dt.now(datetime.timezone.utc)))

        # Find index of next session
        try:
            next_session_index = df[df["end"] > now]["end"].idxmin()
        except ValueError:
            if current["season"]:
                current["current_session"] = str(int(current["current_session"]) + 1)
                current["season"] = False
            if current["current_results"] == current["next_results"]:
                current["next_results"] = None
            if current["current_standings"] == current["next_standings"]:
                current["next_standings"] = None
            return current, None, None

        # Find race id of next weekend
        race_id = df["raceId"][next_session_index]

        # Filter future sessions of next weekend
        df = df[df["raceId"] == race_id]
        df = df[df["end"] > now]

        # Write to current dict
        current["current_session"] = df["sessionId"][next_session_index]

        if df["start"][next_session_index] > now:  # Next session
            current["current_session"] -= 1
        current["current_session"] = str(current["current_session"])

        df_future = df[df["session"].isin(["quali", "spr", "race"])]

        # Update next_results and next_standings sessionId
        if not current.get("current_results") or current["current_results"] == current["next_results"]:
            current["next_results"] = str(df_future["sessionId"].min())

        if not current.get("current_standings") or current["current_standings"] == current["next_standings"]:
            current["next_standings"] = str(df_future["sessionId"].max())

        return current, df, next_session_index

    else:
        return current, None, None


async def fetch_data(current, df):
    idx, update_standings = None, None
    now = int(dt.timestamp(dt.now(datetime.timezone.utc)))

    current_session = current.get("current_session")
    current_results = current.get("current_results")
    next_results = current.get("next_results")
    current_standings = current.get("current_standings")
    next_standings = current.get("next_standings")

    if current_session:
        if next_results:
            # Rate limit to every 5 minutes
            if (int(current_session) > int(next_results) or not current_results) and now % 300 < 60:
                idx = await fetch_results(df, current)

        if next_standings:
            if (int(current_session) > int(next_standings) or not current_standings) and now % 300 < 60:
                update_standings = await fetch_standings()

    return idx, update_standings


async def create_schedule_embed(df, ids, next_session_index):
    if df is not None:
        now = int(dt.timestamp(dt.now(datetime.timezone.utc)))

        # Create embed
        em = discord.Embed(
            title=f"__{df['name'][next_session_index]}__",
            color=ids['color'],
        )

        if df["start"][next_session_index] > now:  # Next session
            em.set_thumbnail(url=os.getenv('THUMBNAIL'))
            em.add_field(
                name=f"__Next session__",
                value=f"{f1_dict['session_names'][df['session'][next_session_index]]} "
                      f"<t:{df['start'][next_session_index]}:R>\n<t:{df['start'][next_session_index]}:F>\n\u200b",
                inline=False
            )
            await bot.change_presence(activity=None, status=None)
        else:  # Current session
            em.set_thumbnail(url=os.getenv('LIVE'))
            em.add_field(
                name=f"Current session__",
                value=f"{f1_dict['session_names'][df['session'][next_session_index]]} "
                      f"<t:{df['start'][next_session_index]}:R>\n<t:{df['start'][next_session_index]}:F>\n\u200b",
                inline=False
            )
            await bot.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name=f"F1: {f1_dict['session_names'][df['session'][next_session_index]]}"
                ),
                status=discord.Status.dnd,
            )

        # Add circuit map
        url = f1_dict["circuit_maps"].get(str(df["circuitId"][next_session_index]))

        # Filter to upcoming events
        df = df[df["session"].isin(["quali", "spr", "race"])]

        # Drop current/next event
        try:
            df.drop([next_session_index], axis=0, inplace=True)
        except KeyError:
            pass

        # Add embed fields
        for _, row in df.iterrows():
            await add_embed_field(em, row)

        if url:
            em.add_field(name="\u200b", value="**__Circuit Map__**")
            em.set_image(url=url)

        return em

    em = discord.Embed(
        title="No F1",
        description="Or more likely, the bot is broken",
        color=ids['color'],
    )
    em.set_thumbnail(url=os.getenv('THUMBNAIL_FAIL'))
    return em


async def create_results_embed(df, ids, idx):
    if os.path.exists("res/results.json"):
        with open("res/results.json", "r") as f:
            results = json.load(f)
    else:
        return None

    data = results["MRData"]["RaceTable"]["Races"][0]
    round_name = df['name'][df[df['round'] == int(data["round"])]['sessionId'].idxmax()]
    session_id = df['sessionId'][df[df['round'] == int(data["round"])]['sessionId'].idxmax()]

    if df['session'][idx] == "quali":
        data = data["QualifyingResults"]

        field = "**#** . . **Driver** . . . . . . . . . . . . . . . . .**Team** . . " \
                "**Q1** . . . . . . . . **Q2** . . . .  . . . . **Q3** . . . . . . \n"
        for i, entry in enumerate(data):
            try:
                padding = f1_dict['embed_padding'][entry['Driver']['code']]
            except KeyError:
                padding = ". " * (19 - len(f"{entry['Driver']['familyName'].upper()}, {entry['Driver']['givenName']}"))
            if entry['position'] == '1':
                field += f"**{entry['position']}** . . ."
            elif entry['position'] == 'D':
                field += f"**DQ** "
            elif entry['position'] == 'N':
                field += f"**NC** "
            elif entry['position'] == 'R':
                field += f"**Ret** "
            elif entry['position'] == 'W':
                field += f"**W** . "
            elif len(entry['position']) == 1:
                field += f"**{entry['position']}** . . "
            elif entry['position'] == '11':
                field += f"**{entry['position']}** . ."
            else:
                field += f"**{entry['position']}** . "
            field += f"_**{entry['Driver']['familyName'].upper()}**, {entry['Driver']['givenName']}_ {padding}" \
                     f"**{f1_dict['team_abbrev'][entry['Constructor']['constructorId']]}**"
            try:
                if len(entry['Q1']):
                    field += f" {f1_dict['abbrev_padding'][entry['Constructor']['constructorId']]}**{entry['Q1']}**"
            except KeyError:
                field += "\n"
            else:
                try:
                    field += f" . . . **{entry['Q2']}**"
                except KeyError:
                    field += "\n"
                else:
                    try:
                        field += f" . . . **{entry['Q3']}**\n"
                    except KeyError:
                        field += "\n"

        embed = discord.Embed(
            title=f"__{round_name}: Qualifying Results__",
            description=field,
            color=ids['color'],
        )

    elif df['session'][idx] == "race" or df['session'][idx] == "spr":
        if df['session'][idx] == "race":
            data = results["MRData"]["RaceTable"]["Races"][0]["Results"]
        else:
            data = results["MRData"]["RaceTable"]["Races"][0]["SprintResults"]

        field = "**#** . . **Driver** . . . . . . . . . . . . . . . . **Team** . . . . . **Points** . . " \
                "**Time** . . . . . . . . \n"
        fastest_lap = None
        for i, entry in enumerate(data):
            try:
                padding = f1_dict['embed_padding'][entry['Driver']['code']]
            except KeyError:
                padding = ". " * (20 - len(f"{entry['Driver']['familyName'].upper()}, {entry['Driver']['givenName']}"))

            if entry['positionText'] == '1':
                field += f"**1** . . ."
            elif entry['positionText'] == 'D':
                field += f"**DQ** "
            elif entry['positionText'] == 'N':
                field += f"**NC** "
            elif entry['positionText'] == 'R':
                field += f"**Ret** "
            elif entry['positionText'] == 'W':
                field += f"**W** . "
            elif len(entry['positionText']) == 1:
                field += f"**{entry['positionText']}** . . "
            elif entry['positionText'] == '11':
                field += f"**11** . ."
            else:
                field += f"**{entry['positionText']}** . "

            field += f"_**{entry['Driver']['familyName'].upper()}**, {entry['Driver']['givenName']}_ {padding}" \
                     f"**{f1_dict['team_abbrev'][entry['Constructor']['constructorId']]}**" \
                     f" . . . {f1_dict['abbrev_padding'][entry['Constructor']['constructorId']]}"

            pts = entry['points']
            pad = 18
            if pts != '0':
                field += f"**+{pts}** "
                pad -= 5 if len(pts) == 1 else 4
                pad -= 2 * len(pts)
                field += f"{pad // 2 * '. '}{pad % 2 * '.'}"
            else:
                if field[-1] == ' ':
                    field += f"{pad // 2 * '. '}{pad % 2 * '.'}"
                else:
                    field += f"{pad // 2 * ' .'}{pad % 2 * ' '}"

            try:
                field += f"**{entry['Time']['time']}**\n"
            except KeyError:
                field += f"**{entry['status']}**\n"

            try:
                lap_rank = entry['FastestLap']['rank']
            except KeyError:
                pass
            else:
                if lap_rank == '1':
                    fastest_lap = f"\u200b\n_Fastest Lap: {entry['Driver']['givenName']} " \
                                  f"**{entry['Driver']['familyName'].upper()}**, " \
                                  f"{entry['FastestLap']['Time']['time']} " \
                                  f"(Lap {entry['FastestLap']['lap']})_"

        if df['session'][idx] == "race" and fastest_lap:
            field += fastest_lap

        if df['session'][idx] == "race":
            embed = discord.Embed(
                title=f"__{round_name}: Race Results__",
                description=field,
                color=ids['color'],
            )
        else:
            embed = discord.Embed(
                title=f"__{round_name}: Sprint Results__",
                description=field,
                color=ids['color'],
            )
    else:
        return None, None

    return embed, session_id


async def create_standings_embed(df, ids):
    if os.path.exists("res/constructors_standings.json"):
        with open("res/constructors_standings.json", "r", encoding="utf-8") as f:
            constructors = json.load(f)
    else:
        return None, None

    if os.path.exists("res/drivers_standings.json"):
        with open("res/drivers_standings.json", "r", encoding="utf-8") as f:
            drivers = json.load(f)
    else:
        return None, None

    data = drivers["MRData"]["StandingsTable"]["StandingsLists"][0]
    round_name = df['name'][df[df['round'] == int(data["round"])]['sessionId'].idxmax()]
    session_id = df['sessionId'][df[df['round'] == int(data["round"])]['sessionId'].idxmax()]

    # Drivers
    embed = discord.Embed(
        title=f"__Standings after: {round_name}__",
        color=ids['color']
    )

    field = ""
    for i, entry in enumerate(data['DriverStandings']):
        padding = f1_dict['embed_padding'][entry['Driver']['code']]

        if entry['positionText'] == '1':
            field += f"**{entry['positionText']}** . . ."
        elif len(entry['positionText']) == 1:
            field += f"**{entry['positionText']}** . . "
        elif entry['positionText'] == '11':
            field += f"**{entry['positionText']}** . ."
        else:
            field += f"**{entry['positionText']}** . "

        field += f"_**{entry['Driver']['familyName'].upper()}**, {entry['Driver']['givenName']}_ " \
                 f"{padding}({entry['points']})\n"

    embed.add_field(
        name="__Drivers' Standings__",
        value=field
    )

    embed.add_field(
        name="\u200b",
        value="\u200b"
    )

    # Constructors
    data = constructors["MRData"]["StandingsTable"]["StandingsLists"][0]
    field = ""
    for i, entry in enumerate(data['ConstructorStandings']):
        try:
            padding = f1_dict['embed_padding'][entry['Constructor']['constructorId']]
        except KeyError:
            padding = ". " * (19 - len(f"{[entry['Constructor']['constructorId']]}"))

        if entry['positionText'] == '1':
            field += f"**{entry['positionText']}** . . ."
        elif len(entry['positionText']) == 1:
            field += f"**{entry['positionText']}** . . "
        else:
            field += f"**{entry['positionText']}** . "

        field += f"_**{entry['Constructor']['name']}**_ {padding}({entry['points']})\n"

    embed.add_field(
        name="__Constructors' Standings__",
        value=field
    )

    return embed, session_id


async def get_ids(text_channel_ids: list):
    text_channel = None
    ids = {
        'guild': None,
        'text': None,
        'voice': None,
        'color': None,
    }

    for c in text_channel_ids:
        try:
            text_channel = await bot.fetch_channel(int(c))
            text_channel_ids.remove(c)
            break
        except discord.NotFound:
            text_channel_ids.remove(c)

    if text_channel:
        ids['text'] = text_channel
        ids['guild'] = text_channel.guild

    '''voice_channel = await get_channel_id(VOICE_CHANNEL_IDS)
    if voice_channel:
        ids['voice'] = voice_channel
        if not ids['text']:
            ids['text'] = voice_channel.guild'''

    ids['color'] = await embed_color(ids['guild'])
    return text_channel_ids, ids


async def update_embeds(embeds, ids):
    for embed in embeds:
        if embed:
            # Set timestamp
            embed.set_footer(text=f"Last updated")
            embed.timestamp = dt.now(datetime.timezone.utc)

    if ids['text']:
        message_history = await ids['text'].history(limit=1).flatten()
        if len(message_history):
            n = 0
            async for msg in ids['text'].history(limit=None):
                if msg.author.id != bot.user.id:
                    await msg.delete()  # delete non-bot messages
                else:
                    n += 1
            if n > 3:
                async for msg in ids['text'].history(limit=None):
                    if n != 3:
                        await msg.delete()  # delete bot messages so there are 3 left
                        n -= 1
            if n == 3:
                message_history = await ids['text'].history(limit=3, oldest_first=True).flatten()
                for i, msg in enumerate(message_history):
                    if embeds[i]:
                        await msg.edit(embed=embeds[i])
            elif n < 3:
                if n == len([x for x in embeds if x]):
                    message_history = await ids['text'].history(limit=2, oldest_first=True).flatten()
                    for i, msg in enumerate(message_history):
                        await msg.edit(embed=[x for x in embeds if x][i])
                elif n < len([x for x in embeds if x]):
                    async for msg in ids['text'].history(limit=2):
                        await msg.delete()
                    for embed in embeds:
                        if embed:
                            await ids['text'].send(embed=embed)
                elif n > len([x for x in embeds if x]) and embeds[2]:
                    message_history = await ids['text'].history(limit=2, oldest_first=True).flatten()
                    for i, msg in enumerate(message_history):
                        if embeds[i] and i == 1:
                            await msg.edit(embed=embeds[2])
        else:
            for embed in embeds:
                if embed:
                    await ids['text'].send(embed=embed)
        return
    else:
        return


# AUXILIARY FUNCTIONS

async def session_adjust(current, df, minutes, message, end=True):
    current_session = current.get("current_session")
    if current_session:
        idx = df[df["sessionId"] >= int(current_session)]["sessionId"].idxmin()
        if end:
            df["end"][idx] += int(minutes * 60)
            changed = "end"
        else:
            df["start"][idx] += int(minutes * 60)
            changed = "start"

        year = df['year'][0]
        df.to_csv(
            f"res/calendars/{year}/{dt.now(datetime.timezone.utc).strftime(f'{year}_calendar_v%y%m%d.csv')}",
            index=False,
        )

        await message.author.send(
            f"{minutes} minutes added to the {changed} time.\nThe scheduled start time is "
            f"{dt.utcfromtimestamp(df['start'][idx]).strftime('%H:%M:%S')} UTC and the scheduled session end time is "
            f"{dt.utcfromtimestamp(df['end'][idx]).strftime('%H:%M:%S')} UTC for the "
            f"{f1_dict['session_names'][df['session'][idx]]} session.")
    else:
        return False


async def get_current() -> dict:
    try:
        f = open("current.json", 'r')
    except FileNotFoundError:
        f = open("current.json", "w")
        current = {}
    else:
        try:
            current = json.load(f)
        except ValueError:
            current = {}
    finally:
        f.close()
        return current


async def fetch_results(df, current):
    if current.get("current_results"):
        idx = df.index[df["sessionId"] == int(current["next_results"])].values[0]
        link = f"{df['year'][idx]}/{df['round'][idx]}/{f1_dict['api_session']['url'][df['session'][idx]]}"
        async with aiohttp.ClientSession() as session:
            print(f"Fetching {f1_dict['session_names'][df['session'][idx]].lower()} results for "
                  f"{df['name'][idx]}...")
            async with session.get(f"https://ergast.com/api/f1/{link}.json") as response:
                results = json.loads(await response.text())
        if not len(results["MRData"]["RaceTable"]["Races"]):
            return None
    elif current.get("current_session"):
        df_filter = df[df["sessionId"] < int(current["current_session"])]
        idx = df_filter[df_filter["session"].isin(["quali", "spr", "race"])]["sessionId"].idxmax()
        async with aiohttp.ClientSession() as session:
            print(f"Fetching {df['name'][idx]} {f1_dict['session_names'][df['session'][idx]]} results...")
            async with session.get(
                f"https://ergast.com/api/f1/{df['year'][idx]}/{df['round'][idx]}/"
                f"{f1_dict['api_session']['url'][df['session'][idx]]}.json"
            ) as response:
                results = json.loads(await response.text())
        if not len(results["MRData"]["RaceTable"]["Races"]):
            return None
    else:
        return None

    with open("res/results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)
    print("Success.")

    return idx


async def fetch_standings():
    async with aiohttp.ClientSession() as session:
        print("Fetching constructors' standings...")
        async with session.get("https://ergast.com/api/f1/current/constructorstandings.json") as response:
            constructors = json.loads(await response.text())
        print("Fetching drivers' standings...")
        async with session.get("https://ergast.com/api/f1/current/driverstandings.json") as response:
            drivers = json.loads(await response.text())

    with open("res/constructors_standings.json", "w", encoding='utf-8') as f:
        json.dump(constructors, f, indent=4)
    with open("res/drivers_standings.json", "w", encoding='utf-8') as f:
        json.dump(drivers, f, indent=4)
    print("Success.")

    return True


async def get_channel_id(channel_ids):
    for c in channel_ids:
        try:
            channel = await bot.fetch_channel(int(c))
            return channel
        except discord.NotFound:
            pass
    return None


async def embed_color(guild: discord.Guild):
    s = str(guild.me.top_role.color) if guild else '#36393F'
    s = int(s.replace("#", ""), 16)
    return int(hex(s), 0)


async def add_embed_field(embed, row):
    embed.add_field(
        name=f"__{f1_dict['session_names'][row['session']]} <t:{row['start']}:R>__",
        value=f"<t:{row['start']}:F>\n\u200b",
        inline=False,
    )
    return embed


@tasks.loop(minutes=1)
async def update():
    if update.current_loop != 0:
        current, df = await fetch_calendar()
        current, df_next, next_session_index = await update_current_params(current, df)
        idx, update_standings = await fetch_data(current, df)

        text_channel_ids = [int(x.strip()) for x in
                            os.getenv('TEXT_CHANNEL_IDS').replace('[', '').replace(']', '').split(',')]
        while len(text_channel_ids):
            text_channel_ids, ids = await get_ids(text_channel_ids)

            em_standings, standings_session_id = None, None
            if update_standings:
                em_standings, standings_session_id = await create_standings_embed(df, ids)

            em_results, results_session_id = None, None
            if idx:
                em_results, results_session_id = await create_results_embed(df, ids, idx)

            em_schedule = await create_schedule_embed(df_next, ids, next_session_index)
            embeds = [em_standings, em_results, em_schedule]
            await update_embeds(embeds, ids)

        if results_session_id:
            current["current_results"] = str(results_session_id)
        if standings_session_id:
            current["current_standings"] = str(standings_session_id)

        with open("current.json", "w", encoding="utf-8") as f:
            json.dump(current, f, indent=4)


if __name__ == '__main__':
    try:
        keep_alive()
        update.start()
        bot.run(TOKEN)
    except discord.errors.HTTPException:
        os.system('kill 1')
