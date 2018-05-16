#
# simple_alarms.py -- simple alarm system for quick n' dirty alarms
#
# eric@naoj.org
#
# -*- coding: utf-8 -*-
#
"""
For adding a quick alarm to Gen2 until it can be absorbed into the
Gen2 alarm handler, or for temporary use.
"""
import sys
import traceback
import time
import threading

from .notifications import SoundNotification


class Alarm(object):
    """Base class for alarms."""

    def __init__(self, key, check_fn=None,
                 priority=10, superceded_by=None):
        super(Alarm, self).__init__()
        # our name, must be unique
        self.key = key
        # optional function to call to check alarm triggered
        self.check_fn = check_fn
        if superceded_by is None:
            superceded_by = []
        # alarms that suppress this alarm, if active
        self.superceded_by = superceded_by
        # priority of this alarm
        self.priority = priority
        # is entire alarm muted?
        self.muted = False

        # time we went active
        self.t_active = 0.0
        # alarm level
        self.level = 0
        # notification dict
        self.notifications = {}

    def check(self, ap):
        """This function is called to check if an alarm's trigger
        condition is true.  It returns an integer level.
        """
        # We default to our passed-in check_fn
        if self.check_fn is None:
            raise ValueError("Please set a check function for this alarm!")

        return self.check_fn(ap, self)

    def add_notifications(self, level, nlst_add):
        """Add notifications to a an alarm.
        """
        nlst = self.get_notifications(level)
        nlst.extend(nlst_add)

    def get_notifications(self, level):
        nlst = self.notifications.setdefault(level, [])
        return nlst


class AlarmProcessor(object):
    """Processes alarms, triggers notifications, handles muting, etc.
    """

    def __init__(self, logger):
        self.logger = logger

        self.cur_time = time.time()
        # set True to mute ALL alarms
        self.global_mute = False
        # set True to mute all SoundNotification's
        self.sound_mute = False

        self.alarm_list = []
        # big dict for all alarms
        self.alarm_dict = {}
        # alarms organized into groups
        self.alarm_group = {}

        self.extras = {}
        # holds status items from status server (set externally)
        self.stat_dict = {}

    def add_alarms(self, alarms, group=None):
        for alarm in alarms:
            if alarm.key in self.alarm_dict:
                raise ValueError("Alarm with key '%s' already exists." % (
                    alarm.key))
            self.alarm_dict[alarm.key] = alarm
            if alarm not in self.alarm_list:
                self.alarm_list.append(alarm)

        # add alarm group, if given
        if group is not None:
            ag = self.alarm_group.setdefault(group, [])
            add_g = [ alarm for alarm in alarms if alarm not in ag ]
            ag.extend(add_g)

        self.alarm_list.sort(key=lambda a: a.priority)

    def get_alarm(self, key):
        return self.alarm_dict[key]

    def get_alarm_group(self, key):
        return self.alarm_group[key]

    def mute(self, *keys):
        """Mute alarms whose keys are passed in as arguments."""
        self.logger.info("muting: %s" % (str(keys)))
        for key in keys:
            alarm = self.get_alarm(key)
            alarm.muted = True

    def unmute(self, *keys):
        """Unmute alarms whose keys are passed in as arguments."""
        self.logger.info("unmuting: %s" % (str(keys)))
        for key in keys:
            alarm = self.get_alarm(key)
            alarm.muted = False

    def check_all_alarms(self):
        self.check_alarms(self.alarm_list)

    def check_alarms(self, alarms):
        """Check alarms in `alarms` and make notifications for ones that
        warrant it.
        """
        self.cur_time = time.time()

        p_alarms = []
        for alarm in alarms:
            try:
                level = alarm.check(self)
                if level != alarm.level:
                    # alarm has changed levels
                    alarm.t_active = self.cur_time
                    alarm.level = level
                    p_alarms.append(alarm)
                elif level > 0:
                    # alarm continues at current level
                    p_alarms.append(alarm)

            except Exception as e:
                self.logger.error("Error checking alarm '%s': %s" % (
                    alarm.key, str(e)))
                try:
                    (type, value, tb) = sys.exc_info()
                    self.logger.error("Traceback:\n%s" % \
                                      "".join(traceback.format_tb(tb)))
                    tb = None

                except Exception as e:
                    self.logger.error("Traceback information unavailable.")

        # process any active alarms or ones that deactivated
        self.process_notifications(p_alarms)

    def process_notifications(self, alarms):
        """Process alarms for notifications."""
        # global mute on?
        if self.global_mute:
            return

        for alarm in alarms:
            # check if this alarm is superceded by others that are
            # active
            if len(alarm.superceded_by) > 0:
                superceded = False
                for a_name in alarm.superceded_by:
                    al = self.alarm_dict[a_name]
                    superceded = superceded or al.active
                if superceded:
                    # this alarm is superceded by another
                    # so don't notify it
                    self.logger.debug("'%s' superceded" % (alarm.key))
                    continue

            if alarm.muted:
                # entire alarm is muted for all notifications
                self.logger.info("muting '%s' per individual mute" % (
                    alarm.key))
                continue

            # get and process notifications for this level
            for nt in alarm.get_notifications(alarm.level):

                # Sound alarm and total sound mute on?
                if isinstance(nt, SoundNotification) and self.sound_mute:
                    self.logger.info("muting '%s' per sound mute" % (
                        alarm.key))
                    continue

                if nt.muted:
                    # this notification instance muted
                    continue

                # interval expired for this notification?
                # TODO: how to decide whether and when to reset the t_notify
                # if the alarm bounces on and off
                if self.cur_time - nt.t_notify >= nt.interval:
                    # time for this notification to proceed
                    nt.t_notify = self.cur_time
                    try:
                        nt.notify(self, alarm)

                    except Exception as e:
                        self.logger.error("notify error in alarm '%s': %s" % (
                            alarm.key, str(e)))
                        try:
                            (type, value, tb) = sys.exc_info()
                            self.logger.error("Traceback:\n%s" % \
                                              "".join(traceback.format_tb(tb)))
                            tb = None

                        except Exception as e:
                            self.logger.error("Traceback information unavailable.")


# END
