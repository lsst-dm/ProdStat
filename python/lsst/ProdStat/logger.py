# This file is part of ProdStat.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Logging utilities for this package."""

import logging
import traceback
import datetime
import yaml


def stream_logger(name, reset=False, level=logging.DEBUG):
    """Add a stream handler to a logger with reasonable configuration.

    Parameters
    ----------
    name : 'str'
        Logger to which to add the stream
    reset : `bool`
        Make sure the new stream is the only handler
    level : `int`
        The log level for the stream.

    Returns
    -------
    logger : `logging.Logger`
        The configured logger
    """
    if isinstance(name, logging.Logger):
        log = name
        name = log.name
    else:
        log = logging.getLogger(name)

    if reset:
        for handler in log.handlers:
            log.removeHandler(handler)

    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setLevel(level)

    # Make sure the level for the logger is set so that messages of the
    # requested level will actually get logged.
    if log.level == 0 or log.level > level:
        log.setLevel(level)

    log_stream_handler.setFormatter(
        logging.Formatter("{asctime} {name} {levelname}: {message}", style="{")
    )
    log.addHandler(log_stream_handler)
    return log


def file_logger(name, fname, reset=False):
    """Add a file handler to a logger with reasonable configuration.

    Parameters
    ----------
    name : 'str'
        Logger to which to add the stream
    fname : `str`
        File into which to log.
    reset : `bool`
        Make sure the new stream is the only handler

    Returns
    -------
    logger : `logging.Logger`
        The configured logger
    """
    if isinstance(name, logging.Logger):
        log = name
        name = log.name
    else:
        log = logging.getLogger(name)

    if reset:
        for handler in log.handlers:
            log.removeHandler(handler)

    log_file_handler = logging.FileHandler(fname)
    log_file_handler.setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log_file_handler.setFormatter(
        logging.Formatter("{asctime} {name} {levelname}: {message}", style="{")
    )
    log.addHandler(log_file_handler)
    return log


# Sometimes you want something between trace and pdb...
class DebugLogger:
    """Callable to log the current line.

    Parameters
    ----------
    logger : `logging.Logger`
        The logger (on name of the logger) to which to log debug information.
    log_level = `int`
        log level at which to log the debug information
    base_fname = `str`
        base of file in which to log debug messages
    stream = `bool`
        Add a stream handler to the logger?
    """

    max_lines = 20
    max_line_len = 80

    def __init__(
        self, logger="debug", log_level=logging.DEBUG, fname_base=None, stream=False
    ):
        self.logger = (
            logger if isinstance(logger, logging.Logger) else logging.getLogger(logger)
        )
        name = self.logger.name

        if fname_base is not None:
            time_string = datetime.datetime.now().isoformat()[:19].replace(":", "")
            fname = fname_base + time_string + ".log"
            file_logger(name, fname, reset=False)
        if stream:
            stream_logger(name, reset=False)
        self.log_level = log_level

    def line(self):
        """Log the current line."""
        stack = traceback.extract_stack()[-2]
        message = "LINE %s:%d (%s)" % tuple(stack)[:3]
        self.logger.log(self.log_level, message)

    def stack(self):
        """Log the stack."""
        raw_traceback = traceback.extract_stack()
        stack_strs = traceback.format_list(raw_traceback)
        for level, stack_str in enumerate(stack_strs[:-1]):
            stack_lines = stack_str.strip().split("\n")
            self.logger.log(
                self.log_level, f"STACK LOCATION {level:d}: {stack_lines[0]}"
            )
            self.logger.log(self.log_level, f"STACK CODE {level:d}: {stack_lines[1]}")

    def logvars(self, **kwargs):
        """Log about a set of variables."""
        stack = traceback.extract_stack()[-2]
        message = "LOGVARS AT %s:%d (%s)" % tuple(stack)[:3]
        self.logger.log(self.log_level, message)

        for varname in kwargs:
            self.logger.log(self.log_level, f"TYPE {varname}: {type(kwargs[varname])}")

            try:
                value_lines = yaml.dump(kwargs[varname], indent=4).split()
            except TypeError:
                value_lines = str(kwargs[varname])

            if isinstance(value_lines, str):
                value_lines = [value_lines]

            truncated_value = len(value_lines) > self.max_lines
            if truncated_value:
                value_lines = value_lines[: self.max_lines]

            for value_line_idx, value_line in enumerate(value_lines):
                truncated_line = len(value_line) > self.max_line_len
                if truncated_line:
                    value_line = value_line[: self.max_line_len - 3] + "..."
                self.logger.log(
                    self.log_level,
                    f"VALUE {varname} {value_line_idx:03d}: {value_line}",
                )

            if truncated_value:
                self.logger.log(self.log_level, f"VALUE {varname} TRUNCATED")
