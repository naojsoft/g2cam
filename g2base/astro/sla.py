#
# sla.py -- Functions converted from Slalib Fortran code
#
# R. Kackley
#
from math import pi
twopi = 2. * pi

# Converted from Slalib sla_DD2TF
def dd2tf(days, precision=2):
    """
    Convert an interval in days into hours, minutes, seconds

    Given:
      angle      interval in days
      precision  number of decimal places of seconds

    Returned:
      sign       '+' or '-'
      hours      number of hours
      minutes    number of minutes
      seconds    number of seconds
      fraction   fractional number of seconds
                 If precision is 2, fraction will
                 be a number between 0 and 99, inclusive

    Notes:

      1)  "precision" less than zero is interpreted as zero.

      2)  The largest useful value for "precision" is determined by the size
          of "days", the format of double precision floating-point numbers
          on the target machine, and the risk of overflowing an integer data type.
          On some architectures, for "days" up to 1.0, the available
          floating-point precision corresponds roughly to NDP=12.
          However, the practical limit is NDP=9, set by the capacity of
          a typical 32-bit integer data type.

      3)  The absolute value of "days" may exceed 1.  In cases where it
          does not, it is up to the caller to test for and handle the
          case where "days" is very nearly 1.0 and rounds up to 24 hours,
          by testing for hours=24 and setting minutes, seconds, fraction all
          to zero.

    From the original Fortran version of sla_DD2TF:

    Copyright P.T.Wallace.  All rights reserved.

    License:
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.

      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.

      You should have received a copy of the GNU General Public License
      along with this program (see SLA_CONDITIONS); if not, write to the
      Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
      Boston, MA  02110-1301  USA

    """

    d2s = 86400.

    #  Handle sign
    if days >= 0.:
        sign = '+'
    else:
        sign = '-'

    if precision < 0:
        nrs = 1.
    else:
        nrs = 10.0 ** precision
    rs = nrs
    rm = rs * 60.
    rh = rm * 60.

    # Round interval and express in smallest units required
    a = round(rs * d2s * abs(days))

    # Separate into fields
    ah = int(a / rh)
    a = a - ah * rh
    am = int(a / rm)
    a = a - am * rm
    asec = int(a / rs)
    af = a - asec * rs

    # Return results
    hours = max(round(ah), 0)
    minutes = max(min(round(am), 59), 0)
    seconds = max(min(round(asec), 59), 0)
    frac = max(round(min(af, rs - 1.)), 0)

    return (sign, hours, minutes, seconds, frac)

# Converted from Slalib sla_DR2TF
def dr2tf(angle, precision):
    """
    Convert an angle in radians to hours, minutes, seconds

    Given:
      angle      angle in radians
      precision  number of decimal places of seconds

    Returned:
      sign       '+' or '-'
      hours      number of hours
      minutes    number of minutes
      seconds    number of seconds
      fraction   fractional number of seconds
                 if precision is 2, fraction will
                 be a number between 0 and 99, inclusive

    From the original Fortran version of sla_DR2TF:

    Copyright P.T.Wallace.  All rights reserved.

    License:
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.

      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.

      You should have received a copy of the GNU General Public License
      along with this program (see SLA_CONDITIONS); if not, write to the
      Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
      Boston, MA  02110-1301  USA

    """

    # Turns to radians
    t2r = twopi

    # Scale then use days to h,m,s routine
    return dd2tf(angle / t2r, precision)

# Converted from Slalib sla_DR2AF
def dr2af(angle, precision):

    """
    Convert an angle in radians to degrees, arcminutes, arcseconds

    Given:
      angle      angle in radians
      precision  number of decimal places of seconds

    Returned:
      sign       '+' or '-'
      degrees    number of degrees
      minutes    number of arcminutes
      seconds    number of arcseconds
      fraction   fractional number of arcseconds
                 if precision is 2, fraction will
                 be a number between 0 and 99, inclusive

    From the original Fortran version of sla_DR2AF:

    Copyright P.T.Wallace.  All rights reserved.

    License:
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.

      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.

      You should have received a copy of the GNU General Public License
      along with this program (see SLA_CONDITIONS); if not, write to the
      Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
      Boston, MA  02110-1301  USA

    """

    # Hours to degrees * radians to turns
    f = 15. / twopi

    # Scale then use days to h,m,s routine
    return dd2tf(angle * f, precision)
