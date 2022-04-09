import aiohttp
import arrow
from bs4 import BeautifulSoup as BS
import csv
from datetime import datetime as dt
import discord #upm package(py-cord)
from discord.ext import tasks #upm package(py-cord)
# from DiscordEvents import DiscordEvents
from f1_dict import reg_weekend, sprint_weekend, name_conv, svg_bypass, circuit_maps
import json
from keep_alive import keep_alive
import os
from PyPDF2 import PdfFileReader
import re

from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
bot_admins = [int(x.strip()) for x in os.getenv('BOT_ADMINS').replace('[', '').replace(']', '').split(',')]
text_channel_ids = [int(x.strip()) for x in
                    os.getenv('TEXT_CHANNEL_IDS').replace('[', '').replace(']', '').split(',')]
voice_channel_ids = [int(x.strip()) for x in os.getenv('VOICE_CHANNEL_IDS').replace('[', '').replace(']', '').split(',')]
intents = discord.Intents.default()
bot = discord.Bot(intents=intents)
# bot.intents.scheduled_events = True


@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')


@bot.event
async def on_message(message):
    if not message.guild and message.author.id in bot_admins and message.content.startswith('session'):
        with open('session_adjust.txt', 'w') as f:
            t = re.search(r'-?\d{1,3}', message.content)
            if t:
                f.write(t.group())
                return
    elif message.author.id != bot.user.id and message.channel.id in text_channel_ids:
        await message.delete()
    return


async def find_latest_pdf(path):
    files = os.listdir(path)
    versions = []
    for i, file in enumerate(files):
        date = re.search(r'\d{2}\.\d{2}\.\d{2}', file)
        if date:
            versions.append([i, int(date.group()[6:8] + date.group()[3:5] + date.group()[0:2])])
    latest = sorted(versions, key=lambda x: x[1])[-1]
    return [path + os.sep + files[latest[0]], latest[1]]


async def pdf_to_csv(year, latest_pdf):
    csvs = os.listdir('session_times')
    pattern = str(latest_pdf[1])
    for x in csvs:
        res = re.search(pattern=pattern, string=x)
        if res:
            with open("latest.txt", 'r') as f:
                if f.read() == latest_pdf[0]:
                    return

    with open(latest_pdf[0], 'rb') as f:
        text = PdfFileReader(f).getPage(0).extractText().split("\n")  # read pdf as string
        text = [x.replace("*", "").strip() for x in text if len(x.replace("*", "").strip())]  # convert to list and cleanup
        race, gp = [], []
        for i, n in enumerate(text):
            race_date = re.match(r'^\d{1,2}-[A-Z][a-z]{2}$', n)  # search for race dates
            if race_date:
                race.append(i)  # save list index of race date

    for i in range(len(race)):
        if i != len(race) - 1:
            gp.append(text[race[i]:race[i + 1]])  # splice list into different grand prix
        else:
            gp.append(text[race[len(race) - 1]:])  # handle index out of range

    for i, n in enumerate(gp):
        if 'SPRINT' in n:
            gp[i] = gp[i][:2] + gp[i][6:9] + gp[i][10:13]  # remove sprint weekend labels and unnecessary info
            gp[i].insert(2, True)  # bool label whether sprint weekend or not
        else:
            gp[i] = gp[i][:5] + gp[i][6:9]  # remove unnecessary info
            gp[i].insert(2, False)  # bool label whether sprint weekend or not
        gp[i].append(int(gp[i].pop(-2).split(":")[0]) - int(gp[i][-1].split(":")[0]))  # time diff
        for j in range(3, 8):
            gp[i][j] = gp[i][j].split()[0].split(":")  # convert string to int time (local)
            if j in range(3, 6):
                gp[i][j][0] = str(int(gp[i][j][0]) - gp[i][-1])  # convert to gmt using time diff
            gp[i][j] = ":".join(gp[i][j])
            ts = arrow.get(f"{year}-{gp[i][0]} {str(gp[i][j])}", "YYYY-D-MMM H:mm").shift(days=(j - 7) // 2)
            # convert to datetime object and adjust for correct session date
            gp[i][j] = ts.shift(seconds=ts.dst().seconds).int_timestamp  # convert to unix timestamp, adjust for dst
        gp[i] = gp[i][1:-1]

    filename = f"session_times{os.sep}{year}_session_times_{latest_pdf[1]}.csv"
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(gp)

    with open(f"latest.txt", 'w') as f:
        f.write(latest_pdf[0])

    return


async def find_latest_csv(path):
    files = os.listdir(path)
    return path + os.sep + (sorted(files))[-1] if files else None


async def read_csv(latest_csv):
    if not latest_csv:
        return None
    with open(latest_csv, 'r') as f:
        reader = csv.reader(f)
        gp = []
        for row in reader:
            gp.append(row)

    for row in gp:
        if row[0] in name_conv.keys():
            row[0] = name_conv[row[0]]
        row[1] = True if row[1] == 'True' else False
        for i in range(2, 7):
            row[i] = int(row[i])
    return gp


async def get_state(gp, ids, t):
    if gp:
        now = arrow.utcnow().int_timestamp
        state, state_2, qualify, race, upcoming_events = None, None, None, None, []
        for i, r in enumerate(gp):
            for j in range(2, 7):
                if j != 6 and gp[i][j] <= now < gp[i][j] + 3600 + t or j == 6 and gp[i][j] <= now < gp[i][j] + 7200 + t:
                    state, state_2, state_3 = "Current", "", ""
                    em = discord.Embed(title=f"__{gp[i][0]} Grand Prix__",
                                       color=ids['color'])
                    em.set_thumbnail(url=os.getenv('LIVE'))
                    if gp[i][1]:
                        await bot.change_presence(
                            activity=discord.Activity(type=discord.ActivityType.watching,
                                                      name=f"F1: {sprint_weekend[j - 2]}"),
                            status=discord.Status.dnd)
                    else:
                        await bot.change_presence(
                            activity=discord.Activity(type=discord.ActivityType.watching,
                                                      name=f"F1: {reg_weekend[j - 2]}"),
                            status=discord.Status.dnd)
                elif gp[i][j] > now:
                    state, state_2, state_3 = "Next", f" <t:{gp[i][j]}:R>", f"\n<t:{gp[i][j]}:F>"
                    em = discord.Embed(title=f"__{gp[i][0]} Grand Prix__",
                                       color=ids['color'])
                    em.set_thumbnail(url=os.getenv('THUMBNAIL'))
                    if t != 0:
                        with open('session_adjust.txt', 'w') as f:
                            f.write('0')
                    await bot.change_presence(activity=None, status=None)
                if state and gp[i][1]:
                    em.add_field(name=f"__{state} session__",
                                 value=f"{sprint_weekend[j - 2]}{state_2}{state_3}\n\u200b",
                                 inline=False)
                    if j < 3:
                        em.add_field(name=f"__{sprint_weekend[1]}__ <t:{gp[i][3]}:R>",
                                     value=f"<t:{gp[i][3]}:F>\n\u200b",
                                     inline=False)
                        upcoming_events.append([f"{gp[i][0]} Grand Prix: {sprint_weekend[1]}", gp[i][3]])
                    if j < 5:
                        em.add_field(name=f"__{sprint_weekend[3]}__ <t:{gp[i][5]}:R>",
                                     value=f"<t:{gp[i][5]}:F>\n\u200b",
                                     inline=False)
                        upcoming_events.append([f"{gp[i][0]} Grand Prix: {sprint_weekend[3]}", gp[i][5]])
                elif state:
                    em.add_field(name=f"__{state} session__",
                                 value=f"{reg_weekend[j - 2]}{state_2}{state_3}\n\u200b",
                                 inline=False)
                    if j < 5:
                        em.add_field(name=f"__{reg_weekend[3]}__ <t:{gp[i][5]}:R>",
                                     value=f"<t:{gp[i][5]}:F>\n\u200b",
                                     inline=False)
                        upcoming_events.append([f"{gp[i][0]} Grand Prix: {reg_weekend[3]}", gp[i][5]])
                if state:
                    if j < 6:
                        em.add_field(name=f"__{reg_weekend[4]}__ <t:{gp[i][6]}:R>",
                                     value=f"<t:{gp[i][6]}:F>\n\u200b",
                                     inline=False)
                        upcoming_events.append([f"{gp[i][0]} Grand Prix: {reg_weekend[4]}", gp[i][6]])
                    return em, gp[i][0], upcoming_events
    em = discord.Embed(title="No F1", description="Or more likely, the bot is broken",
                       color=ids['color'])
    em.set_thumbnail(url=os.getenv('THUMBNAIL_FAIL'))
    em.set_footer(text=f"Last updated")
    em.timestamp = dt.utcnow()
    return em, None, None


async def get_circuit_map(circuit):
    if not circuit:
        return None, ""
    elif circuit in circuit_maps.keys():
        return circuit_maps[circuit], ""
    WIKI_REQUEST = "http://en.wikipedia.org/w/api.php?action=query&prop=pageimages&format=json&piprop=original&titles="
    WIKIMEDIA = "http://commons.wikimedia.org/wiki/File:"
    async with aiohttp.ClientSession() as session:
        if circuit in svg_bypass.keys():
            svg_link = svg_bypass[circuit]
        else:
            async with session.get(f"{WIKI_REQUEST}{circuit.replace(' ', '_')}_Grand_Prix") as response:
                json_data = json.loads(await response.text())
                svg_link = list(json_data['query']['pages'].values())[0]['original']['source'].split('/')[-1]

        async with session.get(f"{WIKIMEDIA}{svg_link}") as response:
            res = await response.text()
            soup = BS(res, "lxml")
            res = list(soup.select('.mw-filepage-other-resolutions > a'))[::-1]
            for r in res:
                url = re.search(r'href="[^"]+">', str(r))
                if url:
                    url = url.group()[6:-2]
                    return url, "Image may be displayed incorrectly in dark mode. Source: Wikipedia\n"


async def update_embeds(em_info, em_map, dark_mode, ids):
    if em_map:
        em_info.add_field(name="\u200b", value="**__Circuit Map__**")
        em_info.set_image(url=em_map)
    em_info.set_footer(text=f"{dark_mode}Last updated")
    em_info.timestamp = dt.utcnow()
    if ids['text']:
        message_history = await ids['text'].history(limit=1).flatten()
        if len(message_history):
            n = 0
            async for msg in ids['text'].history(limit=None):
                if msg.author.id != bot.user.id:
                    await msg.delete()
                else:
                    n += 1
            if n > 1:
                async for msg in ids['text'].history(limit=None):
                    if n != 1:
                        await msg.delete()
                        n -= 1
                    else:
                        await msg.edit(embed=em_info)
            elif n == 1:
                async for msg in ids['text'].history(limit=1):
                    await msg.edit(embed=em_info)
        else:
            await ids['text'].send(embed=em_info)
        return
    else:
        return


'''
async def update_events(upcoming_events, ids):
    curr = await DiscordEvents(TOKEN).list_guild_events(guild_id=ids['guild'].id)
    curr = [x for x in curr if x['creator_id'] == os.getenv('BOT_ID')]
    for i in range(len(upcoming_events)):
        for j in range(len(curr)):
            if curr[j]['name'] == upcoming_events[i][0] and curr[j]['channel_id'] == str(ids['voice'].id) and arrow.get(curr[j]['scheduled_start_time'].replace('T', ''), 'YYYY-MM-DDHH:mm:ssZZ') == arrow.get(str(upcoming_events[i][1]), 'X'):
                continue
        await DiscordEvents(TOKEN).create_guild_event(guild_id=str(ids['guild'].id),
                                                      event_name=upcoming_events[i][0],
                                                      event_start_time=str(arrow.get(str(upcoming_events[i][1]), 'X').format('YYYY-MM-DDTHH:mm:ssZZ')),
                                                      channel_id=str(ids['voice'].id))
'''


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


async def get_session_adjust():
    t = 0
    try:
        with open('session_adjust.txt', 'r') as f:
            t = int(f.read()) * 60
    finally:
        return t


@tasks.loop(minutes=1)
async def update():
    if update.current_loop != 0:
        latest_pdf = await find_latest_pdf(path="start_time_info")
        await pdf_to_csv(int(arrow.utcnow().year), latest_pdf)
        latest_csv = await find_latest_csv(path="session_times")
        gp = await read_csv(latest_csv)

        text_channel_ids = [int(x.strip()) for x in
                            os.getenv('TEXT_CHANNEL_IDS').replace('[', '').replace(']', '').split(',')]
        while len(text_channel_ids):
            ids, text_channel = {'guild': None, 'text': None, 'voice': None, 'color': None}, None
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
            # voice_channel = await get_channel_id(voice_channel_ids)
            # if voice_channel:
            #     ids['voice'] = voice_channel
            #     if not ids['text']:
            #         ids['text'] = voice_channel.guild
            ids['color'] = await embed_color(ids['guild'])

            t = await get_session_adjust()
            em_info, circuit, upcoming_events = await get_state(gp, ids, t)
            em_map, dark_mode = await get_circuit_map(circuit)
            await update_embeds(em_info, em_map, dark_mode, ids)
            # await update_events(upcoming_events, ids)


if __name__ == '__main__':
    try:
        keep_alive()
        update.start()
        bot.run(TOKEN)
    except discord.errors.HTTPException:
        os.system('kill 1')
