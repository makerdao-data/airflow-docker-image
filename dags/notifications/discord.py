import discord_notify as dn
import os


URL = os.environ.get('DISCORD_WEBHOOK')
notifier = dn.Notifier(URL)
