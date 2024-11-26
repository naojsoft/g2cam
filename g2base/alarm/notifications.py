#
# notifications.py -- notifications for alarms, etc.
#
# E. Jeschke
#
# -*- coding: utf-8 -*-
#
class Notification(object):
    """Notification base class for an alarm."""

    def __init__(self, interval=60.0):
        super(Notification, self).__init__()

        # time to wait between notifications
        self.interval = interval
        # time last announced
        self.t_notify = 0.0
        # is notification muted?
        self.muted = False

    def notify(self, ap, alarm):
        raise ValueError("Subclass should implement this method!")


class TTYNotification(Notification):
    """A text notification.  Prints itself to stdout."""

    def __init__(self, text=None, **kwargs):
        super(TTYNotification, self).__init__(**kwargs)
        # text to print for the alarm in terminal
        self.text = text

    def notify(self, ap, alarm):
        # print alarm to terminal
        print('*** %s ***: %s' % (alarm.key, self.text))


class SoundNotification(Notification):
    """Base class for a sound notification."""
    pass


class Gen2SoundFileNotification(SoundNotification):
    """A notification that plays a sound file."""

    def __init__(self, sound_file=None, volume=0, dst='all', **kwargs):
        super(Gen2SoundFileNotification, self).__init__(**kwargs)

        # sound file to play, full path
        self.sound_file = sound_file
        self.volume = volume
        self.priority = 20
        self.dst = dst

    def notify(self, ap, alarm):
        if self.sound_file is not None:
            sndtyp = self.sound_file.split('.')[-1].lower()
            # TODO: add volume
            svc_sound = ap.extras['gen2_sound_svc']
            svc_sound.playFile(self.sound_file, sndtyp, True, None,
                               self.priority, self.dst)


class Gen2TTSNotification(SoundNotification):
    """A notification that uses Text To Speech (TTS)."""

    def __init__(self, sound_text=None,
                 voice='slt', volume=12, dst='all', **kwargs):
        super(Gen2TTSNotification, self).__init__(**kwargs)

        # sound text to synthesize
        self.sound_text = sound_text
        self.voice = voice
        self.volume = volume
        self.priority = 20
        self.dst = dst

    def notify(self, ap, alarm):
        if self.sound_text is not None:
            svc_sound = ap.extras['gen2_sound_svc']
            self.svc_sound.playText(self.sound_text, self.voice,
                                    self.volume, True, None,
                                    self.priority, self.dst)


class PhoneNotification(Notification):
    pass

class TwilioSMSNotification(PhoneNotification):
    """A notification that uses mobile phone text messages (SMS).
    Uses twilio.com's SMS service.
    """

    def __init__(self, sms_text=None, phone_number=None, **kwargs):
        super(TwilioSMSNotification, self).__init__(**kwargs)

        # sound text to synthesize
        self.sms_text = sms_text
        self.phone_number = phone_number

    def notify(self, ap, alarm):
        from twilio.rest import Client

        # Find these values at https://twilio.com/user/account
        account_sid = ap.extras['twilio_account_sid']
        auth_token = ap.extras['twilio_auth_token']
        caller_number = ap.extras['twilio_caller_number']

        if self.sms_text is not None and self.phone_number is not None:
            client = Client(account_sid, auth_token)
            client.api.account.messages.create(
                to=self.phone_number, from_=caller_number,
                body=self.sms_text)


class TwilioRoboCallNotification(PhoneNotification):
    """A notification that uses mobile phone voice robo calls.
    Uses twilio.com's voice service.
    """

    def __init__(self, sound_file=None, volume=0, phone_number=None, **kwargs):
        super(TwilioRoboCallNotification, self).__init__(**kwargs)

        # sound file to play, full path
        self.sound_file = sound_file
        self.volume = volume
        self.phone_number = phone_number

    def notify(self, ap, alarm):
        from twilio.rest import Client

        # Find these values at https://twilio.com/user/account
        account_sid = ap.extras['twilio_account_sid']
        auth_token = ap.extras['twilio_auth_token']
        caller_number = ap.extras['twilio_caller_number']

        if self.sms_text is not None and self.phone_number is not None:
            client = Client(account_sid, auth_token)
            client.calls.create(
                to=self.phone_number, from_=caller_number,
                body=self.sms_text)
