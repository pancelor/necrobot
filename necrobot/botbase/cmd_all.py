from necrobot.botbase.commandtype import CommandType
from necrobot.config import Config, TestLevel


class ForceCommand(CommandType):
    def __init__(self, bot_channel):
        CommandType.__init__(self, bot_channel, 'force')
        self.help_text = '`{0} user command`: Simulate the user entering the given command in the current channel.' \
                         .format(self.mention)

    async def _do_execute(self, cmd):
        if len(cmd.args) < 2:
            await self.client.send_message(
                cmd.channel,
                'Not enough arguments for `{0}`.'.format(self.mention)
            )
            return

        username = cmd.args[0]
        user = self.necrobot.find_member(discord_name=username)
        if user is None:
            await self.client.send_message(
                cmd.channel,
                "Couldn't find the user `{0}`.".format(username)
            )
            return

        message_content = cmd.arg_string[(len(username)+1):]
        await self.necrobot.force_command(channel=cmd.channel, author=user, message_str=message_content)


class Help(CommandType):
    def __init__(self, bot_channel):
        CommandType.__init__(self, bot_channel, 'help')
        self.help_text = 'Help.'

    async def _do_execute(self, cmd):
        args = cmd.args

        # Pop 'verbose' argument
        verbose = False
        for idx, arg in enumerate(args):
            if arg.lstrip('-') == 'verbose':
                args.pop(idx)
                verbose = True

        # List commands if no arguments
        if len(args) == 0:
            command_list_text = ''
            for cmd_type in self.bot_channel.command_types:
                if cmd_type.show_in_help \
                        and (not cmd_type.admin_only or self.bot_channel.is_admin(cmd.author)) \
                        and (not cmd_type.testing_command or Config.TESTING <= TestLevel.TEST):
                    if verbose:
                        command_list_text += '\n`{0}` -- {2}{1}'.format(
                            cmd_type.mention,
                            cmd_type.short_help_text,
                            '[A] ' if cmd_type.admin_only else ''
                        )
                    else:
                        command_list_text += '`{0}`, '.format(cmd_type.mention)
            if not verbose:
                command_list_text = command_list_text[:-2]

            await self.client.send_message(
                cmd.channel,
                'Available commands in this channel: {0}\n\nType `{1} <command>` for more info about a particular '
                'command.'.format(command_list_text, self.mention))

        # Get help text if arguments
        elif len(args) == 1:
            for cmd_type in self.bot_channel.command_types:
                if cmd_type.called_by(args[0]):
                    await self.client.send_message(
                        cmd.channel, '`{0}`: {2}{1}'.format(
                            cmd_type.mention,
                            cmd_type.help_text,
                            '[Admin only] ' if cmd_type.admin_only else ''
                        )
                    )
            return None


class Info(CommandType):
    def __init__(self, bot_channel):
        CommandType.__init__(self, bot_channel, 'info')
        self.help_text = 'Necrobot version information.'

    @property
    def show_in_help(self):
        return False

    async def _do_execute(self, cmd):
        debug_str = ''
        if Config.TESTING == TestLevel.DEBUG:
            debug_str = ' (DEBUG)'
        elif Config.TESTING == TestLevel.TEST:
            debug_str = ' (TEST)'

        await self.bot_channel.client.send_message(
            cmd.channel,
            'Necrobot v-{0}{1}. Type `.help` for a list of commands.'.format(
                Config.BOT_VERSION,
                debug_str
            )
        )