# discord-bot-F1

This bot automatically updates its own message(s) in a dedicated text channel to show
- the schedule of the next race weekend,
- the current drivers' and constructors' standings, and
- the most recent qualifying/sprint/race classification.

The standings and classification come from the Ergast API and the session times are from a copy of the Ergast database.

It is expected that the embed will not display on the first update loop.

In case of session delays due to unforeseen circumstances, bot admins can adjust the start/end time of the current/next session by sending the bot a direct message with the following format:

`session ['start'] <minutes>`
- `['start']`: (str) Include the string 'start' if the start time is to be adjusted. If this is not included, the end time will be adjusted.
- `<minutes>`: (int) Delay for the start/end of the session in minutes. Accepts positive and negative integers.